namespace RabbitSharp.Abstractions
{
    /// <summary>
    /// Interface representing a message with a correlation ID
    /// </summary>
    public interface IMessage
    {
        Guid CorrelationId { get; }
    }
}