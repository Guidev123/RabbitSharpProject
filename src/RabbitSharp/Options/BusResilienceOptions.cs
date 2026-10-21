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
        /// The maximum delay allowed between retry attempts for a failed message delivery.
        /// Default is <c>5 minutes</c>. Kept well under ~24.8 days (int.MaxValue milliseconds) so it
        /// can never overflow the AMQP <c>x-expiration</c> header, which is validated in <see cref="Validate"/>.
        /// </summary>
        public TimeSpan MaxDeliveryRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Validates the configured values, throwing <see cref="ArgumentException"/> for anything that
        /// would otherwise surface as a broker-side 406 PRECONDITION_FAILED (e.g. a negative or
        /// overflowing per-message TTL) or silently misbehave (e.g. a negative retry attempt count).
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
        }
    }
}