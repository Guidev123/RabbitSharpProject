using RabbitSharp.Enums;

namespace RabbitSharp.Abstractions
{
    public interface INamingConventions
    {
        string ExchangeNamingConvention(Type messageType);

        string RoutingKeyNamingConvention(Type messageType, ExchangeTypeEnum exchangeType = ExchangeTypeEnum.Topic);
    }
}