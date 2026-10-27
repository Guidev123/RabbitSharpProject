namespace RabbitSharp.Options
{
    /// <summary>
    /// Defines resilience options for message delivery, including retry behavior and delay settings.
    /// </summary>
    public sealed class BusResilienceOptions
    {
        /// <summary>
        /// The maximum number of retry attempts allowed for message delivery before moving the message to the dead-letter queue.
        /// Default is <c>3</c>.
        /// </summary>
        public int MaxDeliveryRetryAttempts { get; set; } = 3;

        /// <summary>
        /// The initial delay applied before retrying a failed message delivery.
        /// Default is <c>1 second</c>.
        /// </summary>
        public TimeSpan InitialDeliveryRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// The maximum delay allowed between retry attempts for a failed message delivery. Only
        /// applies to <see cref="RetryStrategy.InMemory"/>, whose backoff grows exponentially; a
        /// <see cref="RetryStrategy.RetryQueue"/> delay is constant and ignores this value - see its
        /// remarks.
        /// Default is <c>5 minutes</c>. Kept well under ~24.8 days (int.MaxValue milliseconds) so it
        /// can never overflow the AMQP <c>x-expiration</c> header, which is validated in <see cref="Validate"/>.
        /// </summary>
        public TimeSpan MaxDeliveryRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// How a failed delivery is retried. Default is <see cref="RetryStrategy.InMemory"/>, which
        /// keeps the broker topology to a main queue and a dead-letter queue - no retry queue.
        /// </summary>
        public RetryStrategy Strategy { get; set; } = RetryStrategy.InMemory;

        /// <summary>
        /// The upper bound the worst-case total backoff of <see cref="RetryStrategy.InMemory"/> - the
        /// longest possible delay for every attempt, summed - is allowed to reach. Checked in
        /// <see cref="Validate"/> only when <see cref="Strategy"/> is <see cref="RetryStrategy.InMemory"/>.
        /// </summary>
        /// <remarks>
        /// A delivery under <see cref="RetryStrategy.InMemory"/> stays unacknowledged for its entire
        /// retry window, and RabbitMQ closes the channel out from under an unacknowledged delivery
        /// once it has been outstanding longer than the broker's <c>consumer_timeout</c> (30 minutes
        /// by default, and not configured by this library). This bound exists so a generous
        /// <see cref="MaxDeliveryRetryAttempts"/>/<see cref="MaxDeliveryRetryDelay"/> combination is
        /// caught at configuration time instead of surfacing as a mysteriously dropped subscription in
        /// production. Default is <c>15 minutes</c>, comfortably under the broker's default with room
        /// for processing time on top of the waits themselves. Raise it only alongside the broker's own
        /// <c>consumer_timeout</c>; for a backoff that needs to run longer than that, use
        /// <see cref="RetryStrategy.RetryQueue"/> instead, which does not hold the delivery open while
        /// it waits.
        /// </remarks>
        public TimeSpan MaxInMemoryRetryWindow { get; set; } = TimeSpan.FromMinutes(15);

        /// <summary>
        /// Validates the configured values, throwing <see cref="ArgumentException"/> for anything that
        /// would otherwise surface as a broker-side 406 PRECONDITION_FAILED (e.g. a negative or
        /// overflowing per-message TTL), silently misbehave (e.g. a negative retry attempt count), or
        /// only fail much later in production (e.g. an in-memory backoff long enough to trip the
        /// broker's consumer_timeout).
        /// </summary>
        public void Validate()
        {
            if (MaxDeliveryRetryAttempts < 0)
                throw new ArgumentException("BusResilienceOptions.MaxDeliveryRetryAttempts must be >= 0.", nameof(MaxDeliveryRetryAttempts));

            if (InitialDeliveryRetryDelay <= TimeSpan.Zero)
                throw new ArgumentException("BusResilienceOptions.InitialDeliveryRetryDelay must be > 0.", nameof(InitialDeliveryRetryDelay));

            if (MaxDeliveryRetryDelay < InitialDeliveryRetryDelay)
                throw new ArgumentException("BusResilienceOptions.MaxDeliveryRetryDelay must be >= InitialDeliveryRetryDelay.", nameof(MaxDeliveryRetryDelay));

            if (MaxDeliveryRetryDelay.TotalMilliseconds >= int.MaxValue)
                throw new ArgumentException(
                    "BusResilienceOptions.MaxDeliveryRetryDelay must be less than ~24.8 days (int32 milliseconds), " +
                    "otherwise the retry-queue message TTL overflows and the broker rejects it with 406 PRECONDITION_FAILED.",
                    nameof(MaxDeliveryRetryDelay));

            if (MaxInMemoryRetryWindow <= TimeSpan.Zero)
                throw new ArgumentException("BusResilienceOptions.MaxInMemoryRetryWindow must be > 0.", nameof(MaxInMemoryRetryWindow));

            if (Strategy == RetryStrategy.InMemory)
            {
                var worstCaseMs = GetWorstCaseInMemoryRetryWindowMilliseconds();

                if (worstCaseMs > MaxInMemoryRetryWindow.TotalMilliseconds)
                    throw new ArgumentException(
                        $"BusResilienceOptions: with Strategy = RetryStrategy.InMemory, the worst-case " +
                        $"time a delivery can spend retrying in-process ({worstCaseMs:F0}ms across " +
                        $"{MaxDeliveryRetryAttempts} attempt(s)) exceeds MaxInMemoryRetryWindow " +
                        $"({MaxInMemoryRetryWindow.TotalMilliseconds:F0}ms). The delivery stays " +
                        "unacknowledged for that whole window, and RabbitMQ's consumer_timeout (30 " +
                        "minutes by default) will close the channel if it is exceeded. Lower " +
                        "MaxDeliveryRetryAttempts or MaxDeliveryRetryDelay, raise MaxInMemoryRetryWindow " +
                        "alongside the broker's consumer_timeout, or switch Strategy to " +
                        "RetryStrategy.RetryQueue instead.",
                        nameof(Strategy));
            }
        }

        /// <summary>
        /// The exponential backoff delay, in milliseconds, for the given 1-based attempt number. Used
        /// by <see cref="RetryStrategy.InMemory"/> only - <see cref="RetryStrategy.RetryQueue"/> uses
        /// a constant delay instead, for the reasons given on <see cref="RetryStrategy.RetryQueue"/>.
        /// </summary>
        internal int GetRetryDelayMilliseconds(int attempt)
        {
            var baseDelayMs = InitialDeliveryRetryDelay.TotalMilliseconds;
            var exponentialDelayMs = baseDelayMs * Math.Pow(2, Math.Max(attempt - 1, 0));

            return (int)Math.Min(exponentialDelayMs, MaxDeliveryRetryDelay.TotalMilliseconds);
        }

        private double GetWorstCaseInMemoryRetryWindowMilliseconds()
        {
            double totalMs = 0;

            for (var attempt = 1; attempt <= MaxDeliveryRetryAttempts; attempt++)
            {
                totalMs += GetRetryDelayMilliseconds(attempt);
            }

            return totalMs;
        }
    }
}
