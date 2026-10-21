using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitSharp.Abstractions;
using RabbitSharp.Options;

namespace RabbitSharp.Core
{
    /// <summary>
    /// Defines a service responsible for handling message bus failures,
    /// including retry mechanisms and dead-letter queue processing.
    /// </summary>
    internal interface IBusFailureHandlingService
    {
        /// <summary>
        /// Declares the necessary retry and dead-letter queue infrastructure
        /// for a given message type within the specified bus configuration.
        /// </summary>
        /// <typeparam name="T">
        /// The type of message for which to declare infrastructure. Must implement <see cref="IMessage"/>.
        /// </typeparam>
        /// <param name="options">
        /// The bus infrastructure configuration, including exchange and queue definitions.
        /// </param>
        /// <param name="channel">
        /// The AMQP channel used to declare the necessary infrastructure.
        /// </param>
        /// <param name="cancellationToken">
        /// A token to observe while waiting for the operation to complete. Defaults to <see cref="CancellationToken.None"/>.
        /// </param>
        Task DeclareInfrastructureAsync<T>(BusInfrastructureOptions options, IChannel channel, CancellationToken cancellationToken = default)
            where T : IMessage;

        /// <summary>
        /// Retrieves the number of retry attempts for a given message based on its properties.
        /// </summary>
        /// <param name="properties">
        /// The message properties containing retry count metadata, if present.
        /// </param>
        /// <returns>
        /// The number of retries, or <c>0</c> if no retry count metadata is found.
        /// </returns>
        int GetRetryCount(IReadOnlyBasicProperties? properties);

        /// <summary>
        /// Publishes a message to the retry queue for reprocessing.
        /// </summary>
        /// <param name="eventArgs">
        /// The original message delivery event arguments.
        /// </param>
        /// <param name="queueName">
        /// The base queue name associated with the message.
        /// </param>
        /// <param name="retryCount">
        /// The number of times the message has been retried.
        /// </param>
        /// <param name="correlationId">
        /// The correlation identifier for tracking the message.
        /// </param>
        /// <param name="busResilience">
        /// The bus resilience configuration, including retry policies.
        /// </param>
        /// <param name="channel">
        /// The AMQP channel used to publish the message to the retry queue.
        /// </param>
        /// <param name="cancellationToken">
        /// A token to observe while waiting for the operation to complete. Defaults to <see cref="CancellationToken.None"/>.
        /// </param>
        Task SendToRetryQueueAsync(
            BasicDeliverEventArgs eventArgs,
            string queueName,
            int retryCount,
            string correlationId,
            BusResilienceOptions busResilience,
            IChannel channel,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Publishes a message to the dead-letter queue for storage and inspection.
        /// </summary>
        /// <param name="eventArgs">
        /// The original message delivery event arguments.
        /// </param>
        /// <param name="queueName">
        /// The base queue name associated with the message.
        /// </param>
        /// <param name="correlationId">
        /// The correlation identifier for tracking the message.
        /// </param>
        /// <param name="channel">
        /// The AMQP channel used to publish the message to the dead-letter queue.
        /// </param>
        /// <param name="cancellationToken">
        /// A token to observe while waiting for the operation to complete. Defaults to <see cref="CancellationToken.None"/>.
        /// </param>
        Task SendToDeadLetterQueueAsync(
            BasicDeliverEventArgs eventArgs,
            string queueName,
            string correlationId,
            IChannel channel,
            CancellationToken cancellationToken);
    }
}