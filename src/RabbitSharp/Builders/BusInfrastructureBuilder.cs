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
        /// <remarks>
        /// Every queue overflows with <c>reject-publish</c>: the broker nacks the publish and the
        /// publisher gets a <c>PublishException</c>, instead of <c>drop-head</c> quietly destroying the
        /// oldest message while the publisher is told the publish succeeded. The main queue also gets a
        /// dead-letter exchange at declare time (see <c>BusFailureHandlingService</c>), so overflow, TTL
        /// expiry and rejection all land in the dead-letter queue rather than nowhere.
        /// </remarks>
        private void SetDefaults()
        {
            _config.MainQueue.QueueName = _queueName;
            _config.RetryQueue.QueueName = $"{_queueName}.retry";
            _config.DeadLetterQueue.QueueName = $"{_queueName}.deadletter";

            _config.MainQueue.Arguments = new Dictionary<string, object?>
            {
                {"x-max-length", 50000},
                {"x-overflow", "reject-publish"}
            };

            _config.RetryQueue.Arguments = new Dictionary<string, object?>
            {
                {"x-max-length", 10000},
                {"x-overflow", "reject-publish"}
            };

            _config.DeadLetterQueue.Arguments = new Dictionary<string, object?>
            {
                {"x-max-length", 100000},
                {"x-overflow", "reject-publish"}
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

        /// <summary>Sets the concrete routing key used when publishing.</summary>
        public BusInfrastructureBuilder WithRoutingKey(string routingKey)
        {
            _config.RoutingKey = routingKey;
            return this;
        }

        /// <summary>
        /// Sets the pattern the main queue is bound with, which may use the topic wildcards <c>#</c>
        /// and <c>*</c>. Without it the binding uses the routing key as-is.
        /// </summary>
        public BusInfrastructureBuilder WithBindingPattern(string bindingPattern)
        {
            _config.BindingPattern = bindingPattern;
            return this;
        }

        /// <summary>
        /// Optimizes for high message throughput by raising the main queue's length limit.
        /// </summary>
        /// <remarks>
        /// Does not set <c>x-queue-mode: lazy</c>: that argument has been deprecated and ignored since
        /// RabbitMQ 3.12, which replaced it with per-queue message versions.
        /// </remarks>
        public BusInfrastructureBuilder ForHighThroughput()
        {
            _config.MainQueue.Arguments["x-max-length"] = 200000;
            return this;
        }

        /// <summary>
        /// Ensures ordered message processing: one active consumer at a time, and a main queue that
        /// rejects publishes when full rather than dropping its own head.
        /// </summary>
        /// <remarks>
        /// Ordering still only holds for the first delivery attempt. A message that fails and goes
        /// through the retry queue is re-inserted behind messages published after it - if strict
        /// ordering matters more than retrying, set <c>MaxDeliveryRetryAttempts = 0</c> so failures go
        /// straight to the dead-letter queue.
        /// </remarks>
        public BusInfrastructureBuilder ForOrderedProcessing()
        {
            _config.MainQueue.Arguments["x-single-active-consumer"] = true;
            _config.MainQueue.Arguments["x-overflow"] = "reject-publish";
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
