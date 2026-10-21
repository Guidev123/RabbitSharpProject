using RabbitMQ.Client;
using RabbitSharp.Options;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Testcontainers.RabbitMq;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// Manages one disposable RabbitMQ (with management plugin) container per test class, plus a
    /// management API client for scenarios that need to inspect or disrupt the broker directly
    /// (e.g. forcing a connection closed to exercise automatic recovery).
    /// </summary>
    public sealed class RabbitMqContainerFixture : IAsyncLifetime
    {
        private const string Username = "rabbitmq";
        private const string Password = "rabbitmq";

        private RabbitMqContainer _container = null!;

        public HttpClient ManagementClient { get; private set; } = null!;

        public string AmqpHost => _container.Hostname;

        public int AmqpPort => _container.GetMappedPublicPort(5672);

        public async Task InitializeAsync()
        {
            _container = new RabbitMqBuilder("rabbitmq:3.13-management")
                .WithUsername(Username)
                .WithPassword(Password)
                .WithPortBinding(15672, true)
                .Build();

            await _container.StartAsync();

            ManagementClient = new HttpClient
            {
                BaseAddress = new Uri($"http://{_container.Hostname}:{_container.GetMappedPublicPort(15672)}/")
            };
            ManagementClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes($"{Username}:{Password}")));
        }

        public async Task DisposeAsync()
        {
            ManagementClient?.Dispose();
            await _container.DisposeAsync();
        }

        public void ConfigureBroker(BrokerOptions options, string? clientProvidedName = null)
        {
            options.Hosts = new[] { _container.Hostname };
            options.Port = _container.GetMappedPublicPort(5672);
            options.Username = Username;
            options.Password = Password;
            options.ClientProvidedName = clientProvidedName;
        }

        /// <summary>
        /// Polls the management API's connection list for one carrying the given client-provided
        /// name. The management plugin only samples connections periodically, so this retries for a
        /// few seconds rather than checking once.
        /// </summary>
        public async Task<string?> FindConnectionNameAsync(string clientProvidedName, int maxAttempts = 10)
        {
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                await Task.Delay(1000);

                var json = await ManagementClient.GetStringAsync("api/connections");
                using var doc = JsonDocument.Parse(json);

                foreach (var connection in doc.RootElement.EnumerateArray())
                {
                    if (connection.TryGetProperty("client_properties", out var clientProperties) &&
                        clientProperties.TryGetProperty("connection_name", out var connectionName) &&
                        connectionName.GetString() == clientProvidedName)
                    {
                        return connection.GetProperty("name").GetString();
                    }
                }
            }

            return null;
        }

        public Task ForceCloseConnectionAsync(string managementConnectionName)
        {
            var escaped = Uri.EscapeDataString(managementConnectionName);
            return ManagementClient.DeleteAsync($"api/connections/{escaped}");
        }

        /// <summary>
        /// Opens a connection to the container using the raw RabbitMQ.Client directly, independent
        /// of RabbitSharp's own connection machinery - for assertions that need to inspect broker
        /// state (e.g. reading a queue via <c>BasicGetAsync</c>) without going through the library
        /// under test.
        /// </summary>
        public Task<IConnection> CreateRawConnectionAsync()
        {
            var factory = new ConnectionFactory
            {
                HostName = AmqpHost,
                Port = AmqpPort,
                UserName = Username,
                Password = Password
            };

            return factory.CreateConnectionAsync();
        }
    }
}
