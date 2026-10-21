using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitSharp.Options;
using System.Net.Sockets;

namespace RabbitSharp.Core
{
    internal sealed class ConnectionManager : IConnectionManager
    {
        private readonly BrokerOptions _brokerOptions;
        private readonly ILogger<ConnectionManager> _logger;
        private readonly ConnectionFactory _connectionFactory;
        private readonly SemaphoreSlim _connectionSemaphore = new(1, 1);

        private readonly ResiliencePipeline? _connectRetryPipeline;

        private IConnection? _connection;
        private IChannel? _publishChannel;
        private bool _disposed;

        public ConnectionManager(
            BrokerOptions brokerOptions,
            ILogger<ConnectionManager> logger)
        {
            brokerOptions.Validate();

            _brokerOptions = brokerOptions;
            _logger = logger;
            _connectionFactory = new ConnectionFactory
            {
                UserName = brokerOptions.Username,
                Password = brokerOptions.Password,
                VirtualHost = brokerOptions.VirtualHost,
                RequestedHeartbeat = TimeSpan.FromSeconds(brokerOptions.HeartbeatIntervalSeconds),
                NetworkRecoveryInterval = TimeSpan.FromSeconds(brokerOptions.NetworkRecoveryIntervalInSeconds),
                AutomaticRecoveryEnabled = true,
                ConsumerDispatchConcurrency = brokerOptions.ConsumerDispatchConcurrency,
                ClientProvidedName = brokerOptions.ClientProvidedName
            };

            _connectRetryPipeline = brokerOptions.TryConnectMaxRetries > 0
                ? BuildConnectRetryPipeline(brokerOptions)
                : null;
        }

        private ResiliencePipeline BuildConnectRetryPipeline(BrokerOptions brokerOptions)
        {
            return new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    ShouldHandle = new PredicateBuilder()
                        .Handle<BrokerUnreachableException>()
                        .Handle<IOException>()
                        .Handle<SocketException>(),
                    MaxRetryAttempts = brokerOptions.TryConnectMaxRetries,
                    BackoffType = DelayBackoffType.Exponential,
                    UseJitter = false,
                    DelayGenerator = args =>
                    {
                        _logger.LogWarning("Attempting to connect to RabbitMQ. Retry {RetryCount}/{TryConnectMaxRetries}",
                            args.AttemptNumber + 1, brokerOptions.TryConnectMaxRetries);

                        var delay = TimeSpan.FromSeconds(Math.Pow(2, args.AttemptNumber + 1));
                        var capped = delay > brokerOptions.MaxConnectRetryDelay ? brokerOptions.MaxConnectRetryDelay : delay;
                        return new ValueTask<TimeSpan?>(capped);
                    }
                })
                .Build();
        }

        public bool IsConnected => _connection?.IsOpen == true;

        public async Task EnsureConnectedAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            if (IsConnected)
            {
                return;
            }

            if (_connection is not null)
            {
                await WaitForAutomaticRecoveryAsync(cancellationToken);
                return;
            }

            await _connectionSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_connection is null)
                {
                    await ReconnectAsync(cancellationToken);
                }
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        private async Task WaitForAutomaticRecoveryAsync(CancellationToken cancellationToken)
        {
            using var timeoutCts = new CancellationTokenSource(_brokerOptions.AutomaticRecoveryWaitTimeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            try
            {
                while (!IsConnected)
                {
                    await Task.Delay(100, linkedCts.Token);
                }
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException(
                    $"RabbitMQ connection is down and automatic recovery did not complete within " +
                    $"{_brokerOptions.AutomaticRecoveryWaitTimeout}. The connection was left in place (not replaced) " +
                    "so active subscriptions are not orphaned; increase BrokerOptions.AutomaticRecoveryWaitTimeout " +
                    "if recovery is simply slow, or investigate why automatic recovery is not completing.");
            }
        }

        public async Task<IChannel> GetPublishChannelAsync(CancellationToken cancellationToken = default)
        {
            await EnsureConnectedAsync(cancellationToken);

            if (_publishChannel is { IsOpen: true })
            {
                return _publishChannel;
            }

            await _connectionSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_publishChannel is { IsOpen: true })
                {
                    return _publishChannel;
                }

                await SafeCloseAsync(_publishChannel);
                _publishChannel = await OpenChannelAsync(cancellationToken);
                return _publishChannel;
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        public async Task<IChannel> CreateConsumerChannelAsync(CancellationToken cancellationToken = default)
        {
            await EnsureConnectedAsync(cancellationToken);
            return await OpenChannelAsync(cancellationToken);
        }

        private async Task<IChannel> OpenChannelAsync(CancellationToken cancellationToken)
        {
            var channel = await _connection!.CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true,
                    consumerDispatchConcurrency: _brokerOptions.ConsumerDispatchConcurrency),
                cancellationToken);

            channel.ChannelShutdownAsync += OnChannelShutdownAsync;

            await channel.BasicQosAsync(0, _brokerOptions.PrefetchCount, false, cancellationToken);

            return channel;
        }

        private async Task ReconnectAsync(CancellationToken cancellationToken)
        {
            await SafeCloseAsync(_publishChannel);
            _publishChannel = null;
            await SafeCloseAsync(_connection);
            _connection = null;

            if (_connectRetryPipeline is null)
            {
                await ConnectOnceAsync(cancellationToken);
            }
            else
            {
                await _connectRetryPipeline.ExecuteAsync(
                    ct => new ValueTask(ConnectOnceAsync(ct)),
                    cancellationToken);
            }
        }

        private async Task ConnectOnceAsync(CancellationToken cancellationToken)
        {
            IConnection? connection = null;
            try
            {
                var endpoints = BuildEndpoints();
                connection = await _connectionFactory.CreateConnectionAsync(endpoints, cancellationToken);

                connection.ConnectionShutdownAsync += OnConnectionShutdownAsync;
                connection.RecoverySucceededAsync += OnRecoverySucceededAsync;
                connection.ConnectionRecoveryErrorAsync += OnConnectionRecoveryErrorAsync;

                _connection = connection;
                _publishChannel = await OpenChannelAsync(cancellationToken);

                _logger.LogInformation("Successfully connected to RabbitMQ ({Endpoints})",
                    string.Join(", ", endpoints.Select(e => $"{e.HostName}:{e.Port}")));
            }
            catch
            {
                if (connection is not null)
                {
                    await SafeCloseAsync(connection);
                }

                _connection = null;
                _publishChannel = null;
                throw;
            }
        }

        private List<AmqpTcpEndpoint> BuildEndpoints()
        {
            var endpoints = new List<AmqpTcpEndpoint>(_brokerOptions.Hosts.Length);

            foreach (var host in _brokerOptions.Hosts)
            {
                var (hostName, port) = ParseHostAndPort(host, _brokerOptions.Port);

                var sslOption = _brokerOptions.UseSsl
                    ? new SslOption { Enabled = true, ServerName = _brokerOptions.SslServerName ?? hostName }
                    : new SslOption { Enabled = false };

                endpoints.Add(new AmqpTcpEndpoint(hostName, port, sslOption));
            }

            return endpoints;
        }

        /// <summary>
        /// Parses a <c>"host"</c> or <c>"host:port"</c> entry (and the bracketed IPv6 forms
        /// <c>"[::1]"</c> / <c>"[::1]:5672"</c>), falling back to <paramref name="defaultPort"/> when
        /// no port is present.
        /// </summary>
        internal static (string Host, int Port) ParseHostAndPort(string host, int defaultPort)
        {
            if (host.Length > 0 && host[0] == '[')
            {
                var closeBracket = host.IndexOf(']');
                if (closeBracket > 0)
                {
                    var hostPart = host[1..closeBracket];
                    var rest = host[(closeBracket + 1)..];

                    if (rest.Length > 1 && rest[0] == ':' && int.TryParse(rest.AsSpan(1), out var ipv6Port))
                    {
                        return (hostPart, ipv6Port);
                    }

                    return (hostPart, defaultPort);
                }
            }

            var firstColon = host.IndexOf(':');
            var lastColon = host.LastIndexOf(':');

            if (firstColon > 0 && firstColon == lastColon && int.TryParse(host.AsSpan(lastColon + 1), out var port))
            {
                return (host[..lastColon], port);
            }

            return (host, defaultPort);
        }

        private Task OnChannelShutdownAsync(object sender, ShutdownEventArgs args)
        {
            if (args.ReplyCode != Constants.ReplySuccess)
            {
                _logger.LogWarning(
                    "RabbitMQ channel closed unexpectedly: {ReplyCode} {ReplyText}",
                    args.ReplyCode, args.ReplyText);
            }

            return Task.CompletedTask;
        }

        private Task OnConnectionShutdownAsync(object sender, ShutdownEventArgs args)
        {
            if (args.ReplyCode != Constants.ReplySuccess)
            {
                _logger.LogWarning(
                    "RabbitMQ connection shut down unexpectedly: {ReplyCode} {ReplyText}. Automatic recovery will attempt to reconnect.",
                    args.ReplyCode, args.ReplyText);
            }

            return Task.CompletedTask;
        }

        private Task OnRecoverySucceededAsync(object sender, AsyncEventArgs args)
        {
            _logger.LogInformation("RabbitMQ connection automatically recovered.");
            return Task.CompletedTask;
        }

        private Task OnConnectionRecoveryErrorAsync(object sender, ConnectionRecoveryErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "RabbitMQ automatic connection recovery failed.");
            return Task.CompletedTask;
        }

        private static async Task SafeCloseAsync(IChannel? channel)
        {
            if (channel is null)
            {
                return;
            }

            try
            {
                if (channel.IsOpen)
                {
                    await channel.CloseAsync();
                }
            }
            catch
            {
            }
            finally
            {
                await channel.DisposeAsync();
            }
        }

        private static async Task SafeCloseAsync(IConnection? connection)
        {
            if (connection is null)
            {
                return;
            }

            try
            {
                if (connection.IsOpen)
                {
                    await connection.CloseAsync();
                }
            }
            catch
            {
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(ConnectionManager));
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            await SafeCloseAsync(_publishChannel);
            await SafeCloseAsync(_connection);

            _connectionSemaphore.Dispose();
        }
    }
}
