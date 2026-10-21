using RabbitSharp.Options;

namespace RabbitSharp.Builders
{
    /// <summary>
    /// Builds the message bus infrastructure configuration.
    /// </summary>
    public sealed class BusInfrastructureBuilder
    {
        private readonly BusInfrastructureOptions _config = new();
        private readonly string _queueName;

        /// <summary>
        /// Initializes with the base queue name.
        /// </summary>
        public BusInfrastructureBuilder(string queueName)
        {
            _queueName = queueName;
            SetDefaults();
        }

        /// <summary>
        /// Sets default queue values.
        /// </summary>
        private void SetDefaults()
        {
            _config.MainQueue.QueueName = _queueName;
            _config.RetryQueue.QueueName = $"{_queueName}.retry";
            _config.DeadLetterQueue.QueueName = $"{_queueName}.deadletter";

            _config.MainQueue.Arguments = new Dictionary<string, object?>
            {
                {"x-max-length", 50000},
                {"x-overflow", "drop-head"}
            };

            _config.RetryQueue.Arguments = new Dictionary<string, object?>
            {
                {"x-max-length", 10000},
                {"x-overflow", "reject-publish"}
            };

            _config.DeadLetterQueue.Arguments = new Dictionary<string, object?>
            {
                {"x-max-length", 100000},
                {"x-overflow", "drop-head"}
            };
        }

        /// <summary>Configures the exchange.</summary>
        public BusInfrastructureBuilder WithExchange(Action<ExchangeBuilder> configure)
        {
            var builder = new ExchangeBuilder(_config.Exchange);
            configure(builder);
            return this;
        }

        /// <summary>Configures the main queue.</summary>
        public BusInfrastructureBuilder WithMainQueue(Action<QueueBuilder> configure)
        {
            var builder = new QueueBuilder(_config.MainQueue);
            configure(builder);
            return this;
        }

        /// <summary>Configures the retry queue.</summary>
        public BusInfrastructureBuilder WithRetryQueue(Action<QueueBuilder> configure)
        {
            var builder = new QueueBuilder(_config.RetryQueue);
            configure(builder);
            return this;
        }

        /// <summary>Configures the dead-letter queue.</summary>
        public BusInfrastructureBuilder WithDeadLetterQueue(Action<QueueBuilder> configure)
        {
            var builder = new QueueBuilder(_config.DeadLetterQueue);
            configure(builder);
            return this;
        }

        /// <summary>Sets the routing key.</summary>
        public BusInfrastructureBuilder WithRoutingKey(string routingKey)
        {
            _config.RoutingKey = routingKey;
            return this;
        }

        /// <summary>Optimizes for high message throughput.</summary>
        public BusInfrastructureBuilder ForHighThroughput()
        {
            _config.MainQueue.Arguments["x-max-length"] = 200000;
            _config.MainQueue.Arguments["x-queue-mode"] = "lazy";
            return this;
        }

        /// <summary>Ensures ordered message processing.</summary>
        public BusInfrastructureBuilder ForOrderedProcessing()
        {
            _config.MainQueue.Arguments["x-single-active-consumer"] = true;
            return this;
        }

        /// <summary>Configures for development environment.</summary>
        public BusInfrastructureBuilder ForDevelopment()
        {
            _config.MainQueue.Arguments["x-max-length"] = 1000;
            _config.MainQueue.Arguments["x-expires"] = 3600000;
            _config.RetryQueue.Arguments["x-expires"] = 1800000;
            return this;
        }

        /// <summary>Builds and returns the configuration.</summary>
        public BusInfrastructureOptions Build() => _config;
    }
}