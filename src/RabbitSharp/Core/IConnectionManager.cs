using RabbitMQ.Client;

namespace RabbitSharp.Core
{
    internal interface IConnectionManager : IDisposable, IAsyncDisposable
    {
        Task EnsureConnectedAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Returns the single channel shared by all <c>PublishAsync</c> calls. Lazily (re)created
        /// when missing or closed. Publisher confirmations are enabled on it.
        /// </summary>
        Task<IChannel> GetPublishChannelAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Creates a brand-new channel dedicated to one subscription. Keeping each subscription on
        /// its own channel means a declare failure or protocol-level error (e.g. a 406 from
        /// mismatched queue arguments) on one subscription closes only that channel - it cannot take
        /// down publishing or any other subscription sharing the same connection.
        /// </summary>
        Task<IChannel> CreateConsumerChannelAsync(CancellationToken cancellationToken = default);

        bool IsConnected { get; }
    }
}
