using RabbitSharp.Enums;

namespace RabbitSharp.Abstractions
{
    public class SimpleNamingConventions : INamingConventions
    {
        public virtual string ExchangeNamingConvention(Type messageType)
        {
            return messageType.Name.ToLowerInvariant();
        }

        public virtual string RoutingKeyNamingConvention(Type messageType, ExchangeTypeEnum exchangeType = ExchangeTypeEnum.Topic)
        {
            var typeName = messageType.Name;

            return exchangeType switch
            {
                ExchangeTypeEnum.Direct => typeName,
                ExchangeTypeEnum.Topic => $"{typeName}.#",
                ExchangeTypeEnum.Fanout => string.Empty,
                _ => typeName
            };
        }
    }
}