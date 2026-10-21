using RabbitSharp.Enums;
using RabbitSharp.Extensions;
using RabbitSharp.Options;

namespace RabbitSharp.Builders
{
    /// <summary>
    /// Helps configure a queue.
    /// </summary>
    public sealed class QueueBuilder
    {
        private readonly QueueOptions _config;

        public QueueBuilder(QueueOptions config)
        {
            _config = config;
        }

        /// <summary>Sets the queue name.</summary>
        public QueueBuilder WithName(string name)
        {
            _config.QueueName = name;
            return this;
        }

        /// <summary>Marks the queue as durable.</summary>
        public QueueBuilder AsDurable(bool durable = true)
        {
            _config.Durable = durable;
            return this;
        }

        /// <summary>Marks the queue as exclusive.</summary>
        public QueueBuilder AsExclusive(bool exclusive = true)
        {
            _config.Exclusive = exclusive;
            return this;
        }

        /// <summary>Enables auto-delete for the queue.</summary>
        public QueueBuilder WithAutoDelete(bool autoDelete = true)
        {
            _config.AutoDelete = autoDelete;
            return this;
        }

        /// <summary>Sets the maximum queue length.</summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="maxLength"/> is negative.</exception>
        public QueueBuilder WithMaxLength(int maxLength)
        {
            if (maxLength < 0)
                throw new ArgumentOutOfRangeException(nameof(maxLength), maxLength, "Queue max length must be >= 0.");

            _config.Arguments["x-max-length"] = maxLength;
            return this;
        }

        /// <summary>Sets the message time-to-live.</summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="ttl"/> is negative, or exceeds ~24.8 days (int32 milliseconds),
        /// which would overflow the AMQP <c>x-message-ttl</c> argument and be rejected by the broker.
        /// </exception>
        public QueueBuilder WithTTL(TimeSpan ttl)
        {
            ValidateMillisecondRange(ttl, nameof(ttl));
            _config.Arguments["x-message-ttl"] = (int)ttl.TotalMilliseconds;
            return this;
        }

        /// <summary>Sets the queue expiration time.</summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="expiration"/> is negative, or exceeds ~24.8 days (int32 milliseconds).
        /// </exception>
        public QueueBuilder WithExpiration(TimeSpan expiration)
        {
            ValidateMillisecondRange(expiration, nameof(expiration));
            _config.Arguments["x-expires"] = (int)expiration.TotalMilliseconds;
            return this;
        }

        /// <summary>Sets the maximum priority level.</summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown if <paramref name="priority"/> is outside the AMQP-supported range of 1-255.
        /// </exception>
        public QueueBuilder WithMaxPriority(int priority)
        {
            if (priority is < 1 or > 255)
                throw new ArgumentOutOfRangeException(nameof(priority), priority, "Queue max priority must be between 1 and 255.");

            _config.Arguments["x-max-priority"] = priority;
            return this;
        }

        private static void ValidateMillisecondRange(TimeSpan value, string paramName)
        {
            if (value < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(paramName, value, "Value must be >= 0.");

            if (value.TotalMilliseconds >= int.MaxValue)
                throw new ArgumentOutOfRangeException(paramName, value, "Value must be less than ~24.8 days (int32 milliseconds).");
        }

        /// <summary>Sets the overflow behavior.</summary>
        public QueueBuilder WithOverflowBehavior(OverFlowStrategyEnum behavior = OverFlowStrategyEnum.RejectPublish)
        {
            _config.Arguments["x-overflow"] = behavior.GetEnumDescription();
            return this;
        }

        /// <summary>Enables or disables single active consumer.</summary>
        public QueueBuilder WithSingleActiveConsumer(bool enable = true)
        {
            if (enable)
                _config.Arguments["x-single-active-consumer"] = true;
            else
                _config.Arguments.Remove("x-single-active-consumer");
            return this;
        }

        /// <summary>Sets the dead-letter exchange and optional routing key.</summary>
        public QueueBuilder WithDeadLetterExchange(string exchange, string? routingKey = null)
        {
            _config.Arguments["x-dead-letter-exchange"] = exchange;

            if (!string.IsNullOrWhiteSpace(routingKey))
                _config.Arguments["x-dead-letter-routing-key"] = routingKey;

            return this;
        }

        /// <summary>Adds a custom queue argument.</summary>
        public QueueBuilder WithCustomArgument(string key, object? value)
        {
            _config.Arguments[key] = value;
            return this;
        }

        /// <summary>Marks the queue as lazy.</summary>
        /// <remarks>
        /// <c>x-queue-mode</c> has been deprecated and ignored since RabbitMQ 3.12, which replaced it
        /// with per-queue message versions, so this sets an argument modern brokers do nothing with.
        /// </remarks>
        [Obsolete("x-queue-mode is deprecated and ignored since RabbitMQ 3.12; the broker decides what to keep in memory on its own.")]
        public QueueBuilder AsLazyQueue()
        {
            _config.Arguments["x-queue-mode"] = "lazy";
            return this;
        }

        /// <summary>
        /// Configures the queue for high-volume processing: a high length limit, and a publish that is
        /// rejected once that limit is reached rather than silently destroying the oldest message.
        /// </summary>
        public QueueBuilder ForHighVolume()
        {
            return WithMaxLength(100000)
                   .WithOverflowBehavior(OverFlowStrategyEnum.RejectPublish);
        }
    }
}
