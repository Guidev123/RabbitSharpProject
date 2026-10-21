using RabbitMQ.Client;
using RabbitSharp.Options;
using System.Net;
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

        /// <summary>
        /// The broker image, overridable so the suite can be run against the versions users actually
        /// deploy - RabbitMQ 4.x changes defaults and drops support for arguments 3.13 still accepts.
        /// </summary>
        private const string DefaultImage = "rabbitmq:3.13-management";
        private const string ImageEnvironmentVariable = "RABBITSHARP_TEST_BROKER_IMAGE";

        private RabbitMqContainer _container = null!;

        public HttpClient ManagementClient { get; private set; } = null!;

        public string AmqpHost => _container.Hostname;

        public int AmqpPort => _container.GetMappedPublicPort(5672);

        public static string Image =>
            Environment.GetEnvironmentVariable(ImageEnvironmentVariable) is { Length: > 0 } image ? image : DefaultImage;

        public async Task InitializeAsync()
        {
            _container = new RabbitMqBuilder(Image)
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

        /// <summary>
        /// How many channels the broker has open right now, across the whole node, read with
        /// <c>rabbitmqctl</c> rather than the management API: the management plugin only samples
        /// periodically, and a leaked channel the client is about to finalize shows up or not
        /// depending on that timing. This is the only place a leaked channel is visible - one that is
        /// never closed and that nothing holds a reference to any more stays open here.
        /// </summary>
        public async Task<int> CountBrokerChannelsAsync()
        {
            var result = await _container.ExecAsync(new[] { "rabbitmqctl", "list_channels", "-q", "name" });

            if (result.ExitCode != 0)
            {
                throw new InvalidOperationException($"rabbitmqctl list_channels failed: {result.Stderr}");
            }

            return result.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Length;
        }

        /// <summary>
        /// How many channels the broker currently has open on a connection, as the management API
        /// reports them, or <c>-1</c> when the connection itself is gone.
        /// </summary>
        public async Task<int> CountChannelsAsync(string managementConnectionName)
        {
            var escaped = Uri.EscapeDataString(managementConnectionName);
            var response = await ManagementClient.GetAsync($"api/connections/{escaped}/channels");

            // A connection that is gone makes this sub-resource answer 500, not 404.
            if (response.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.InternalServerError)
            {
                return -1;
            }

            response.EnsureSuccessStatusCode();

            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            return doc.RootElement.GetArrayLength();
        }

        public Task ForceCloseConnectionAsync(string managementConnectionName)
        {
            var escaped = Uri.EscapeDataString(managementConnectionName);
            return ManagementClient.DeleteAsync($"api/connections/{escaped}");
        }

        /// <summary>
        /// The declared arguments of a queue, as the broker reports them - the authoritative answer to
        /// what the library actually asked for.
        /// </summary>
        public async Task<Dictionary<string, string>> GetQueueArgumentsAsync(string queueName)
        {
            var response = await ManagementClient.GetAsync($"api/queues/%2F/{Uri.EscapeDataString(queueName)}");

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                return new Dictionary<string, string>();
            }

            response.EnsureSuccessStatusCode();

            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());

            var arguments = new Dictionary<string, string>();

            if (doc.RootElement.TryGetProperty("arguments", out var declared))
            {
                foreach (var argument in declared.EnumerateObject())
                {
                    arguments[argument.Name] = argument.Value.ToString();
                }
            }

            return arguments;
        }

        public async Task<bool> QueueExistsAsync(string queueName)
        {
            var response = await ManagementClient.GetAsync($"api/queues/%2F/{Uri.EscapeDataString(queueName)}");
            return response.StatusCode != HttpStatusCode.NotFound;
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

        /// <summary>
        /// The current depth of a queue, read over AMQP so it reflects the broker now rather than the
        /// management plugin's last sample.
        /// </summary>
        public async Task<uint> GetQueueDepthAsync(string queueName)
        {
            await using var connection = await CreateRawConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            return await channel.MessageCountAsync(queueName);
        }
    }
}
