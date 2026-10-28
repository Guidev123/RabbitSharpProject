namespace RabbitSharp.Abstractions
{
    /// <summary>
    /// An optional, purely compile-time convenience for a message type: implementing it documents the
    /// <c>CorrelationId</c> property at the call site and catches a missing one at build time.
    /// </summary>
    /// <remarks>
    /// Nothing in RabbitSharp requires a message type to implement this any more - every
    /// <c>PublishAsync</c>/<c>SubscribeAsync</c> overload only needs <c>T</c> to be a reference type
    /// exposing a public <c>Guid CorrelationId { get; }</c> property, read by convention at runtime
    /// (see <see cref="Core.MessageCorrelationIdAccessor"/>). That is what lets a project that only
    /// defines message contracts - shared between a producer and its consumers - skip a reference to
    /// RabbitSharp, and everything it pulls in, entirely: a plain class or record with that one
    /// property is a valid RabbitSharp message on its own.
    /// </remarks>
    public interface IMessage
    {
        Guid CorrelationId { get; }
    }
}
