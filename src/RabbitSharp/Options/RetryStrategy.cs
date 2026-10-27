namespace RabbitSharp.Options
{
    /// <summary>
    /// How a failed delivery is retried before it is dead-lettered.
    /// </summary>
    public enum RetryStrategy
    {
        /// <summary>
        /// The failed delivery is retried in-process: the consumer awaits the backoff delay with
        /// <see cref="Task.Delay(int)"/> and invokes the handler again on the same delivery, without
        /// publishing anywhere. No retry queue is declared or used.
        /// </summary>
        /// <remarks>
        /// This is the default. It keeps the broker topology down to a main queue and a dead-letter
        /// queue, at a cost: the delivery - and the prefetch slot it occupies - stays unacknowledged
        /// for the whole backoff window, and retry progress is not durable. If the process restarts
        /// (or the subscription is cancelled) mid-backoff, the delivery is requeued and retried from
        /// attempt 0 on its next delivery, instead of resuming where it left off.
        /// <see cref="BusResilienceOptions.MaxInMemoryRetryWindow"/> guards against the worst case
        /// (the longest possible backoff, summed across every attempt) running into the broker's
        /// <c>consumer_timeout</c>, which closes the channel from under a delivery that has been
        /// unacknowledged for too long.
        /// </remarks>
        InMemory,

        /// <summary>
        /// The failed delivery is acknowledged and republished to a single physical retry queue, with
        /// a fixed per-message TTL equal to <see cref="BusResilienceOptions.InitialDeliveryRetryDelay"/>,
        /// which dead-letters it back to the main queue once that TTL expires.
        /// </summary>
        /// <remarks>
        /// Retry progress survives a process restart, and the delivery's prefetch slot is freed while
        /// it waits - but the backoff is constant rather than exponential. RabbitMQ only evaluates a
        /// queue's head for expiry, so a single queue cannot hold a growing per-attempt delay without
        /// a later, shorter-delay message waiting behind an earlier, longer one;
        /// <see cref="BusResilienceOptions.MaxDeliveryRetryDelay"/> is not used by this strategy.
        /// The delay is carried per message rather than as a fixed queue argument, so it can be
        /// changed between deployments without the broker rejecting a redeclare of the queue.
        /// </remarks>
        RetryQueue
    }
}
