namespace RabbitSharp.Options
{
    /// <summary>
    /// Represents configuration options for RabbitMQ messaging infrastructure,
    /// including exchange, main queue, retry queue, dead-letter queue, and routing key.
    /// </summary>
    public sealed class BusInfrastructureOptions
    {
        /// <summary>
        /// Gets or sets the exchange configuration.
        /// </summary>
        public ExchangeOptions Exchange { get; set; } = new();

        /// <summary>
        /// Gets or sets the main queue configuration.
        /// </summary>
        public QueueOptions MainQueue { get; set; } = new();

        /// <summary>
        /// Gets or sets the retry queue configuration. Its name is the base name of the per-delay
        /// retry queues the library declares (<c>{name}.1000ms</c>, <c>{name}.2000ms</c>, ...), one per
        /// distinct backoff delay, each with a fixed <c>x-message-ttl</c>.
        /// </summary>
        public QueueOptions RetryQueue { get; set; } = new();

        /// <summary>
        /// Gets or sets the dead-letter queue configuration.
        /// </summary>
        public QueueOptions DeadLetterQueue { get; set; } = new();

        /// <summary>
        /// Gets or sets the concrete routing key used when publishing.
        /// Can be <see langword="null"/> to use the configured naming convention.
        /// </summary>
        public string? RoutingKey { get; set; }

        /// <summary>
        /// Gets or sets the pattern the main queue is bound with, which may use the topic wildcards
        /// <c>#</c> and <c>*</c>. Left <see langword="null"/>, the binding falls back to
        /// <see cref="RoutingKey"/>, so a subscription that only sets a routing key keeps binding with
        /// exactly that key.
        /// </summary>
        public string? BindingPattern { get; set; }
    }
}
