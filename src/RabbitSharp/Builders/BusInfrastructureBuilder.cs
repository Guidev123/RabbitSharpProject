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
        /// Initializes with the base queue name, which every other queue name is derived from.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="queueName"/> is <see langword="null"/>, empty or whitespace. A
        /// blank name is not an empty name to AMQP - see
        /// <see cref="Options.BusInfrastructureOptions.Validate"/>.
        /// </exception>
        public BusInfrastructureBuilder(string queueName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

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
        /// Ordering only holds fully under <see cref="BusResilienceOptions.Strategy"/>
        /// <see cref="RetryStrategy.InMemory"/> (the default), where a failed message is retried in
        /// place and later messages simply wait behind it. Under <see cref="RetryStrategy.RetryQueue"/>
        /// a failed message is republished and re-inserted behind messages published after it - if
        /// strict ordering matters more than retrying there, set <c>MaxDeliveryRetryAttempts = 0</c>
        /// so failures go straight to the dead-letter queue instead.
        /// </remarks>
        public BusInfrastructureBuilder ForOrderedProcessing()
        {
            _config.MainQueue.Arguments["x-single-active-consumer"] = true;
            _config.MainQueue.Arguments["x-overflow"] = "reject-publish";
            return this;
        }

        /// <summary>
        /// Configures for a development environment: a small main queue that is deleted after an hour
        /// with no consumer, so a dev broker does not fill up with queues from branches nobody is
        /// running any more.
        /// </summary>
        /// <remarks>
        /// Deleting the main queue deletes the messages in it, so this is for development only - an
        /// application that is down for over an hour comes back to an empty queue, and whatever was
        /// published in the meantime was rejected as unroutable.
        /// <para>
        /// The retry queue (declared only under <see cref="RetryStrategy.RetryQueue"/>) deliberately
        /// does not expire. <c>x-expires</c> counts a queue as unused when it has no consumers, has
        /// not been redeclared and has not been read with <c>basic.get</c> - and publishing to it does
        /// not count. The retry queue is only ever published to, so "unused" is its normal state even
        /// while the subscription is perfectly healthy: it would be deleted out from under a working
        /// subscription, taking any message waiting out its TTL with it, and every later retry would
        /// fail to route and be dead-lettered instead of retried.
        /// </para>
        /// </remarks>
        public BusInfrastructureBuilder ForDevelopment()
        {
            _config.MainQueue.Arguments["x-max-length"] = 1000;
            _config.MainQueue.Arguments["x-expires"] = 3600000;
            return this;
        }

        /// <summary>Builds and returns the configuration.</summary>
        public BusInfrastructureOptions Build() => _config;
    }
}
