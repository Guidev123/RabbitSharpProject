using RabbitSharp.Enums;

namespace RabbitSharp.Abstractions
{
    public interface INamingConventions
    {
        string ExchangeNamingConvention(Type messageType);

        /// <summary>
        /// The routing key a message of <paramref name="messageType"/> is <em>published</em> with. This
        /// must be a concrete key, never a binding pattern: wildcards carry no meaning in a publish and
        /// would only ever match a queue bound with that exact literal string.
        /// </summary>
        string RoutingKeyNamingConvention(Type messageType, ExchangeTypeEnum exchangeType = ExchangeTypeEnum.Topic);

        /// <summary>
        /// The pattern a queue is <em>bound</em> with for <paramref name="messageType"/>, which may use
        /// the topic wildcards <c>#</c> and <c>*</c>. Defaults to
        /// <see cref="RoutingKeyNamingConvention"/> so existing implementations keep compiling and keep
        /// their current behaviour.
        /// </summary>
        string BindingPatternNamingConvention(Type messageType, ExchangeTypeEnum exchangeType = ExchangeTypeEnum.Topic)
            => RoutingKeyNamingConvention(messageType, exchangeType);
    }
}
