using RabbitSharp.Enums;

namespace RabbitSharp.Options
{
    /// <summary>
    /// Represents configuration options for a RabbitMQ exchange.
    /// </summary>
    public sealed class ExchangeOptions
    {
        /// <summary>
        /// Gets or sets the name of the exchange.
        /// Can be <see langword="null"/> for the default exchange.
        /// </summary>
        public string? ExchangeName { get; set; }

        /// <summary>
        /// Gets or sets the type of the exchange.
        /// Defaults to <see cref="ExchangeTypeEnum.Topic"/>.
        /// </summary>
        public ExchangeTypeEnum Type { get; set; } = ExchangeTypeEnum.Topic;

        /// <summary>
        /// Gets or sets a value indicating whether the exchange should survive a broker restart.
        /// Defaults to <c>true</c>.
        /// </summary>
        public bool Durable { get; set; } = true;

        /// <summary>
        /// Gets or sets additional exchange arguments used for advanced RabbitMQ configurations.
        /// </summary>
        public Dictionary<string, object?> Arguments { get; set; } = new();
    }
}