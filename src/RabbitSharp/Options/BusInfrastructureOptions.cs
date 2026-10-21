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
        /// Gets or sets the retry queue configuration.
        /// </summary>
        public QueueOptions RetryQueue { get; set; } = new();

        /// <summary>
        /// Gets or sets the dead-letter queue configuration.
        /// </summary>
        public QueueOptions DeadLetterQueue { get; set; } = new();

        /// <summary>
        /// Gets or sets the routing key to be used for message publishing.
        /// Can be <see langword="null"/> to use a default routing configuration.
        /// </summary>
        public string? RoutingKey { get; set; }
    }
}