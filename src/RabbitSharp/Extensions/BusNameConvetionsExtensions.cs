using RabbitSharp.Abstractions;
using RabbitSharp.Enums;

namespace RabbitSharp.Extensions
{
    internal static class BusNameConvetionsExtensions
    {
        public static string GetExchangeName<T>(this INamingConventions conventions) where T : class
        {
            return conventions.ExchangeNamingConvention(typeof(T));
        }

        public static string GetRoutingKey<T>(this INamingConventions conventions, ExchangeTypeEnum exchangeType = ExchangeTypeEnum.Topic) where T : class
        {
            return conventions.RoutingKeyNamingConvention(typeof(T), exchangeType);
        }

        public static string GetBindingPattern<T>(this INamingConventions conventions, ExchangeTypeEnum exchangeType = ExchangeTypeEnum.Topic) where T : class
        {
            return conventions.BindingPatternNamingConvention(typeof(T), exchangeType);
        }
    }
}
