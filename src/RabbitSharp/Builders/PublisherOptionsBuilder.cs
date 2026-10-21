using RabbitSharp.Enums;
using RabbitSharp.Options;

namespace RabbitSharp.Builders
{
    /// <summary>
    /// Provides a fluent API for configuring and building <see cref="PublisherOptions"/> instances.
    /// </summary>
    public sealed class PublisherOptionsBuilder
    {
        private readonly PublisherOptions _config = new();

        /// <summary>
        /// Configures the exchange settings for the publisher.
        /// </summary>
        /// <param name="configure">
        /// An action that receives an <see cref="ExchangeBuilder"/> to define the exchange configuration.
        /// </param>
        /// <returns>
        /// The current <see cref="PublisherOptionsBuilder"/> instance for method chaining.
        /// </returns>
        public PublisherOptionsBuilder WithExchange(Action<ExchangeBuilder> configure)
        {
            var builder = new ExchangeBuilder(_config.Exchange);
            configure(builder);
            return this;
        }

        /// <summary>
        /// Sets the routing key to be used when publishing messages.
        /// </summary>
        /// <param name="routingKey">
        /// The routing key that determines how messages are routed by the exchange.
        /// </param>
        /// <returns>
        /// The current <see cref="PublisherOptionsBuilder"/> instance for method chaining.
        /// </returns>
        public PublisherOptionsBuilder WithRoutingKey(string routingKey)
        {
            _config.RoutingKey = routingKey;
            return this;
        }

        /// <summary>
        /// Adds a custom AMQP header to be attached to the published message.
        /// </summary>
        public PublisherOptionsBuilder WithHeader(string key, object? value)
        {
            _config.Headers ??= new Dictionary<string, object?>();
            _config.Headers[key] = value;
            return this;
        }

        /// <summary>
        /// Builds and returns the configured <see cref="PublisherOptions"/> instance.
        /// </summary>
        /// <remarks>
        /// A routing key is required for <see cref="ExchangeTypeEnum.Direct"/> and
        /// <see cref="ExchangeTypeEnum.Topic"/> exchanges, but is legitimately empty for
        /// <see cref="ExchangeTypeEnum.Fanout"/> - which is exactly what
        /// <see cref="Abstractions.SimpleNamingConventions"/> produces for fanout, so that
        /// convention must not be rejected here.
        /// </remarks>
        /// <returns>
        /// A fully configured <see cref="PublisherOptions"/> instance.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the exchange name is not set, or if a routing key is required but missing.
        /// </exception>
        public PublisherOptions Build()
        {
            if (string.IsNullOrWhiteSpace(_config.Exchange.ExchangeName))
            {
                throw new InvalidOperationException("Exchange name must be set before building the options.");
            }

            if (_config.Exchange.Type != ExchangeTypeEnum.Fanout && string.IsNullOrEmpty(_config.RoutingKey))
            {
                throw new InvalidOperationException(
                    $"Routing key must be set before building the options for a {_config.Exchange.Type} exchange.");
            }

            _config.RoutingKey ??= string.Empty;

            return _config;
        }
    }
}