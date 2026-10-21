namespace RabbitSharp.Options
{
    /// <summary>
    /// Represents configuration options for a RabbitMQ queue.
    /// </summary>
    public sealed class QueueOptions
    {
        /// <summary>
        /// Gets or sets the name of the queue.
        /// Defaults to <see cref="string.Empty"/>.
        /// </summary>
        public string QueueName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets additional queue arguments used for advanced RabbitMQ configurations.
        /// For example, message TTL, maximum length, or dead-letter exchange.
        /// </summary>
        public Dictionary<string, object?> Arguments { get; set; } = new Dictionary<string, object?>();

        /// <summary>
        /// Gets or sets a value indicating whether the queue should survive a broker restart.
        /// Defaults to <c>true</c>.
        /// </summary>
        public bool Durable { get; set; } = true;

        /// <summary>
        /// Gets or sets a value indicating whether the queue should be used exclusively
        /// by the connection that declares it.
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool Exclusive { get; set; } = false;

        /// <summary>
        /// Gets or sets a value indicating whether the queue should be automatically deleted
        /// when there are no more consumers.
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool AutoDelete { get; set; } = false;
    }
}