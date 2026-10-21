using RabbitSharp.Enums;
using RabbitSharp.Options;

namespace RabbitSharp.Builders
{
    /// <summary>
    /// Helps configure the exchange.
    /// </summary>
    public sealed class ExchangeBuilder
    {
        private readonly ExchangeOptions _config;

        public ExchangeBuilder(ExchangeOptions config)
        {
            _config = config;
        }

        /// <summary>Sets the exchange name.</summary>
        public ExchangeBuilder WithName(string name)
        {
            _config.ExchangeName = name;
            return this;
        }

        /// <summary>Sets the exchange type.</summary>
        public ExchangeBuilder WithType(ExchangeTypeEnum type)
        {
            _config.Type = type;
            return this;
        }

        /// <summary>Marks the exchange as durable.</summary>
        public ExchangeBuilder AsDurable(bool durable = true)
        {
            _config.Durable = durable;
            return this;
        }

        /// <summary>Adds a custom exchange argument.</summary>
        public ExchangeBuilder WithArgument(string key, object? value)
        {
            _config.Arguments[key] = value;
            return this;
        }
    }
}