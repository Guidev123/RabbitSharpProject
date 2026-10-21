namespace RabbitSharp.Options
{
    /// <summary>
    /// Defines connection settings for the RabbitMQ broker.
    /// </summary>
    public sealed class BrokerOptions
    {
        /// <summary>
        /// The maximum number of attempts to reconnect to the broker if the initial connection fails.
        /// Only transient failures are retried: a wrong password or a missing virtual host fails
        /// immediately instead of spending the whole backoff budget on something that cannot succeed.
        /// Default is <c>5</c>.
        /// </summary>
        public int TryConnectMaxRetries { get; set; } = 5;

        /// <summary>
        /// The upper bound applied to the exponential backoff between initial-connection retry attempts.
        /// Without a cap, retry N waits 2^N seconds indefinitely; this clamps that growth.
        /// Default is <c>2 minutes</c>.
        /// </summary>
        public TimeSpan MaxConnectRetryDelay { get; set; } = TimeSpan.FromMinutes(2);

        /// <summary>
        /// How long to wait for RabbitMQ.Client's own automatic connection recovery to bring an
        /// already-established connection back up before giving up on that call with a
        /// <see cref="TimeoutException"/>. Only applies after the first successful connect - the
        /// initial connect is governed by <see cref="TryConnectMaxRetries"/>/<see cref="MaxConnectRetryDelay"/>
        /// instead. Default is <c>30 seconds</c>.
        /// <para>
        /// A publish issued while the connection is down blocks for up to this long, so a request-path
        /// publish inherits it as its own latency; lower it, or use
        /// <see cref="RecoveryFailFastWindow"/>, when failing fast matters more than riding out a
        /// short outage.
        /// </para>
        /// </summary>
        public TimeSpan AutomaticRecoveryWaitTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Once a wait for automatic recovery has timed out, every further call fails immediately for
        /// this long instead of each one paying the full <see cref="AutomaticRecoveryWaitTimeout"/> -
        /// the circuit stays open for this window, and the first call after it tries again. The first
        /// successful connection closes it early.
        /// Set to <see cref="TimeSpan.Zero"/> to disable fail-fast and have every call wait.
        /// Default is <c>5 seconds</c>.
        /// </summary>
        public TimeSpan RecoveryFailFastWindow { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// The interval, in seconds, between reconnection attempts.
        /// Default is <c>10 seconds</c>.
        /// </summary>
        public int NetworkRecoveryIntervalInSeconds { get; set; } = 10;

        /// <summary>
        /// The heartbeat interval, in seconds, used to detect and maintain the connection's health.
        /// Default is <c>60 seconds</c>.
        /// </summary>
        public int HeartbeatIntervalSeconds { get; set; } = 60;

        /// <summary>
        /// The list of RabbitMQ hostnames or IP addresses to connect to. Required.
        /// A host entry may include a port as <c>"host:port"</c>; otherwise <see cref="Port"/> is used.
        /// </summary>
        public string[] Hosts { get; set; } = null!;

        /// <summary>
        /// The port used for hosts in <see cref="Hosts"/> that do not specify their own port.
        /// Default is <c>-1</c>, which resolves to the AMQP protocol default (5672, or 5671 when <see cref="UseSsl"/> is enabled).
        /// </summary>
        public int Port { get; set; } = -1;

        /// <summary>
        /// Enables TLS for all broker connections. Default is <c>false</c>.
        /// </summary>
        public bool UseSsl { get; set; }

        /// <summary>
        /// The server name used for TLS certificate validation (SNI) when <see cref="UseSsl"/> is enabled.
        /// When not set, each host name in <see cref="Hosts"/> is used.
        /// </summary>
        public string? SslServerName { get; set; }

        /// <summary>
        /// The username used for authentication with the broker. Required.
        /// </summary>
        public string Username { get; set; } = null!;

        /// <summary>
        /// The password used for authentication with the broker. Required.
        /// </summary>
        public string Password { get; set; } = null!;

        /// <summary>
        /// The RabbitMQ virtual host to connect to.
        /// Default is <c>"/"</c>.
        /// </summary>
        public string VirtualHost { get; set; } = "/";

        /// <summary>
        /// The prefetch count for message consumers, determining how many messages can be sent to a consumer
        /// before it must ack/nack, per channel. Default is <c>10</c>.
        /// </summary>
        public ushort PrefetchCount { get; set; } = 10;

        /// <summary>
        /// The number of messages the client will dispatch to consumer callbacks concurrently, per channel.
        /// Values greater than <c>1</c> remove the guarantee that messages are handled in delivery order,
        /// and consumer callbacks must be safe to run concurrently.
        /// Default is <c>1</c> (sequential dispatch).
        /// </summary>
        public ushort ConsumerDispatchConcurrency { get; set; } = 1;

        /// <summary>
        /// A client-provided name surfaced in the RabbitMQ Management UI, so connections from this
        /// application can be told apart from others. Strongly recommended in production.
        /// </summary>
        public string? ClientProvidedName { get; set; }

        /// <summary>
        /// Validates required fields, throwing <see cref="ArgumentException"/> with a precise message
        /// instead of letting a misconfiguration surface as a <see cref="NullReferenceException"/> deep
        /// inside <c>ConnectionFactory</c>, or as a DNS failure far from the malformed host entry that
        /// caused it.
        /// </summary>
        public void Validate()
        {
            if (Hosts is null || Hosts.Length == 0)
                throw new ArgumentException("BrokerOptions.Hosts must contain at least one host.", nameof(Hosts));

            if (Array.Exists(Hosts, string.IsNullOrWhiteSpace))
                throw new ArgumentException("BrokerOptions.Hosts must not contain null or empty entries.", nameof(Hosts));

            if (Port != -1 && !BrokerEndpointParser.IsValidPort(Port))
                throw new ArgumentException(
                    $"BrokerOptions.Port must be -1 (the AMQP protocol default) or between " +
                    $"{BrokerEndpointParser.MinPort} and {BrokerEndpointParser.MaxPort}.",
                    nameof(Port));

            foreach (var host in Hosts)
            {
                try
                {
                    BrokerEndpointParser.Parse(host, Port);
                }
                catch (ArgumentException ex)
                {
                    throw new ArgumentException($"BrokerOptions.Hosts: {ex.Message}", nameof(Hosts), ex);
                }
            }

            if (string.IsNullOrWhiteSpace(Username))
                throw new ArgumentException("BrokerOptions.Username must be provided.", nameof(Username));

            if (Password is null)
                throw new ArgumentException("BrokerOptions.Password must be provided.", nameof(Password));

            if (TryConnectMaxRetries < 0)
                throw new ArgumentException("BrokerOptions.TryConnectMaxRetries must be >= 0.", nameof(TryConnectMaxRetries));

            if (NetworkRecoveryIntervalInSeconds < 0)
                throw new ArgumentException("BrokerOptions.NetworkRecoveryIntervalInSeconds must be >= 0.", nameof(NetworkRecoveryIntervalInSeconds));

            if (HeartbeatIntervalSeconds < 0)
                throw new ArgumentException("BrokerOptions.HeartbeatIntervalSeconds must be >= 0.", nameof(HeartbeatIntervalSeconds));

            if (MaxConnectRetryDelay <= TimeSpan.Zero)
                throw new ArgumentException("BrokerOptions.MaxConnectRetryDelay must be > 0.", nameof(MaxConnectRetryDelay));

            if (AutomaticRecoveryWaitTimeout <= TimeSpan.Zero)
                throw new ArgumentException("BrokerOptions.AutomaticRecoveryWaitTimeout must be > 0.", nameof(AutomaticRecoveryWaitTimeout));

            if (RecoveryFailFastWindow < TimeSpan.Zero)
                throw new ArgumentException("BrokerOptions.RecoveryFailFastWindow must be >= 0.", nameof(RecoveryFailFastWindow));

            if (PrefetchCount == 0)
                throw new ArgumentException("BrokerOptions.PrefetchCount must be > 0.", nameof(PrefetchCount));

            if (ConsumerDispatchConcurrency < 1)
                throw new ArgumentException("BrokerOptions.ConsumerDispatchConcurrency must be >= 1.", nameof(ConsumerDispatchConcurrency));
        }
    }
}
