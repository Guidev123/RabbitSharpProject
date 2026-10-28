using RabbitSharp.Enums;
using System.Text.Json;

namespace RabbitSharp.Options
{
    /// <summary>
    /// Defines how messages are serialized, validated and routed by the convention-based overloads
    /// (<c>PublishAsync&lt;T&gt;(message)</c> and <c>SubscribeAsync&lt;T&gt;(queueName, handler)</c>).
    /// </summary>
    public sealed class BusMessagingOptions
    {
        /// <summary>
        /// The exchange type used by the convention-based publish and subscribe overloads, which must
        /// agree with each other: publishing to an exchange already declared with a different type is
        /// rejected by the broker with 406 PRECONDITION_FAILED. Change this when the convention-based
        /// overloads should use Fanout or Direct instead.
        /// Default is <see cref="ExchangeTypeEnum.Topic"/>.
        /// </summary>
        public ExchangeTypeEnum DefaultExchangeType { get; set; } = ExchangeTypeEnum.Topic;

        /// <summary>
        /// The options used to serialize published bodies and to deserialize consumed ones.
        /// <see cref="JsonSerializerOptions.PropertyNameCaseInsensitive"/> is enabled by default so a
        /// camelCase payload from a non-.NET producer binds to its properties instead of silently
        /// deserializing into an all-defaults instance.
        /// </summary>
        public JsonSerializerOptions SerializerOptions { get; set; } = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        /// <summary>
        /// When <see langword="true"/> (the default), a message that deserializes with its
        /// <c>CorrelationId</c> property equal to <see cref="Guid.Empty"/> is treated as an unreadable
        /// payload and dead-lettered instead of being handed to the handler and acked as processed -
        /// the usual symptom of a payload whose property names did not bind.
        /// True is the safe default precisely because a producer cannot be trusted to always populate
        /// <c>CorrelationId</c> correctly - an empty one is the most reliable signal available that a
        /// payload never bound. Set this to <see langword="false"/> only for a message type that
        /// legitimately never carries a meaningful correlation id.
        /// </summary>
        public bool RejectMessagesWithEmptyCorrelationId { get; set; } = true;

        /// <summary>
        /// Validates the configured values, throwing <see cref="ArgumentException"/> for anything that
        /// would otherwise surface much later as a broker error or a silently unreadable payload.
        /// </summary>
        public void Validate()
        {
            if (SerializerOptions is null)
                throw new ArgumentException("BusMessagingOptions.SerializerOptions must not be null.", nameof(SerializerOptions));

            if (!Enum.IsDefined(typeof(ExchangeTypeEnum), DefaultExchangeType))
                throw new ArgumentException(
                    $"BusMessagingOptions.DefaultExchangeType has an undefined value ({(int)DefaultExchangeType}).",
                    nameof(DefaultExchangeType));
        }
    }
}