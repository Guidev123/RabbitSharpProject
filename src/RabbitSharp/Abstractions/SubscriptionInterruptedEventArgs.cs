namespace RabbitSharp.Abstractions
{
    /// <summary>
    /// Describes a subscription that stopped consuming for a reason the application cannot otherwise
    /// observe: the broker cancelled the consumer (the queue was deleted, a policy changed, the queue
    /// expired, a cluster failover moved it) or the subscription's channel closed unexpectedly.
    /// Neither case throws anywhere, so without handling this the queue simply grows with no error.
    /// </summary>
    public sealed class SubscriptionInterruptedEventArgs : EventArgs
    {
        public SubscriptionInterruptedEventArgs(
            string queueName,
            string? consumerTag,
            SubscriptionInterruptionReason reason,
            int? replyCode = null,
            string? replyText = null)
        {
            QueueName = queueName;
            ConsumerTag = consumerTag;
            Reason = reason;
            ReplyCode = replyCode;
            ReplyText = replyText;
        }

        /// <summary>The queue the subscription was consuming from.</summary>
        public string QueueName { get; }

        /// <summary>The consumer tag, when the broker reported one.</summary>
        public string? ConsumerTag { get; }

        /// <summary>Why the subscription stopped consuming.</summary>
        public SubscriptionInterruptionReason Reason { get; }

        /// <summary>The AMQP reply code, for a channel shutdown.</summary>
        public int? ReplyCode { get; }

        /// <summary>The AMQP reply text, for a channel shutdown.</summary>
        public string? ReplyText { get; }
    }

    /// <summary>Why a subscription stopped consuming.</summary>
    public enum SubscriptionInterruptionReason
    {
        /// <summary>The broker sent <c>basic.cancel</c> for this consumer.</summary>
        ConsumerCancelledByBroker,

        /// <summary>The subscription's channel closed with a non-success reply code.</summary>
        ChannelShutdown
    }
}
