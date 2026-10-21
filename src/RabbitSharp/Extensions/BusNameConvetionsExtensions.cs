using RabbitSharp.Abstractions;
using RabbitSharp.Enums;

namespace RabbitSharp.Extensions
{
    internal static class BusNameConvetionsExtensions
    {
        public static string GetExchangeName<T>(this INamingConventions conventions) where T : IMessage
        {
            return conventions.ExchangeNamingConvention(typeof(T));
        }

        public static string GetRoutingKey<T>(this INamingConventions conventions, ExchangeTypeEnum exchangeType = ExchangeTypeEnum.Topic) where T : IMessage
        {
            return conventions.RoutingKeyNamingConvention(typeof(T), exchangeType);
        }
    }
}