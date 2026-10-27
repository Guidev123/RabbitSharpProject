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
        /// Gets or sets the retry queue configuration. Only declared, and only used, when
        /// <see cref="BusResilienceOptions.Strategy"/> is <see cref="RetryStrategy.RetryQueue"/> - see
        /// its remarks for why it is a single physical queue rather than one per backoff delay.
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

        /// <summary>
        /// Validates the queue names, which AMQP gives a meaning to that no exception would otherwise
        /// surface.
        /// </summary>
        /// <remarks>
        /// A blank queue name is not an empty name to AMQP: in <c>queue.bind</c> and
        /// <c>basic.consume</c> it means "the last queue declared on this channel". The declares run
        /// main queue, dead-letter queue, then the retry queue (when one is declared at all - see
        /// <see cref="RetryStrategy"/>), so a blank main queue name makes the binding and the consumer
        /// silently attach to another queue while the queue the subscription was meant to serve is a
        /// server-named one nothing is bound to. The subscription comes up and consumes the wrong
        /// queue, and nothing throws.
        /// </remarks>
        /// <exception cref="ArgumentException">Thrown when a required queue name is missing.</exception>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="busResilience"/> is <see langword="null"/>.
        /// </exception>
        public void Validate(BusResilienceOptions busResilience)
        {
            ArgumentNullException.ThrowIfNull(busResilience);

            if (string.IsNullOrWhiteSpace(MainQueue.QueueName))
                throw new ArgumentException(
                    "BusInfrastructureOptions.MainQueue.QueueName must be provided. A blank name means " +
                    "\"the last queue declared on this channel\" to AMQP, so the binding and the consumer " +
                    "would silently attach to another queue instead.",
                    nameof(MainQueue));

            if (string.IsNullOrWhiteSpace(DeadLetterQueue.QueueName))
                throw new ArgumentException(
                    "BusInfrastructureOptions.DeadLetterQueue.QueueName must be provided. It is also the " +
                    "main queue's x-dead-letter-routing-key, so a blank name routes every dead-lettered " +
                    "message to the default exchange with no routing key, where the broker drops it.",
                    nameof(DeadLetterQueue));

            if (busResilience.MaxDeliveryRetryAttempts > 0
                && busResilience.Strategy == RetryStrategy.RetryQueue
                && string.IsNullOrWhiteSpace(RetryQueue.QueueName))
                throw new ArgumentException(
                    "BusInfrastructureOptions.RetryQueue.QueueName must be provided when " +
                    "BusResilienceOptions.MaxDeliveryRetryAttempts is greater than zero and " +
                    "BusResilienceOptions.Strategy is RetryStrategy.RetryQueue.",
                    nameof(RetryQueue));
        }
    }
}
