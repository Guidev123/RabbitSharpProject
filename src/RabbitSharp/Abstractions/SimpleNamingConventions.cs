using RabbitSharp.Enums;

namespace RabbitSharp.Abstractions
{
    public class SimpleNamingConventions : INamingConventions
    {
        public virtual string ExchangeNamingConvention(Type messageType)
        {
            return messageType.Name.ToLowerInvariant();
        }

        /// <summary>
        /// The concrete publish key: the message type name for Direct and Topic, empty for Fanout.
        /// </summary>
        public virtual string RoutingKeyNamingConvention(Type messageType, ExchangeTypeEnum exchangeType = ExchangeTypeEnum.Topic)
        {
            return exchangeType switch
            {
                ExchangeTypeEnum.Fanout => string.Empty,
                _ => messageType.Name
            };
        }

        /// <summary>
        /// The binding pattern: <c>Type.#</c> on a topic exchange, so a subscriber also receives the
        /// sub-keys a conventional producer publishes (<c>Type.created</c>, <c>Type.v2</c>, ...) while
        /// still matching the bare <c>Type</c> key this library publishes.
        /// </summary>
        public virtual string BindingPatternNamingConvention(Type messageType, ExchangeTypeEnum exchangeType = ExchangeTypeEnum.Topic)
        {
            var typeName = messageType.Name;

            return exchangeType switch
            {
                ExchangeTypeEnum.Topic => $"{typeName}.#",
                ExchangeTypeEnum.Fanout => string.Empty,
                _ => typeName
            };
        }
    }
}
