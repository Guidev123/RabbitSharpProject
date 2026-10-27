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
        /// <param name="busResilience">
        /// The retry configuration. The retry queue is only declared when <see cref="BusResilienceOptions.Strategy"/>
        /// is <see cref="RetryStrategy.RetryQueue"/> and <see cref="BusResilienceOptions.MaxDeliveryRetryAttempts"/>
        /// is greater than zero; under <see cref="RetryStrategy.InMemory"/> no retry queue is declared at all.
        /// </param>
        /// <param name="channel">
        /// The AMQP channel used to declare the necessary infrastructure.
        /// </param>
        /// <param name="cancellationToken">
        /// A token to observe while waiting for the operation to complete. Defaults to <see cref="CancellationToken.None"/>.
        /// </param>
        Task DeclareInfrastructureAsync<T>(
            BusInfrastructureOptions options,
            BusResilienceOptions busResilience,
            IChannel channel,
            CancellationToken cancellationToken = default)
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
        /// Publishes a message to the retry queue, under <see cref="RetryStrategy.RetryQueue"/>, for
        /// reprocessing once that queue's per-message TTL expires.
        /// </summary>
        /// <param name="eventArgs">
        /// The original message delivery event arguments.
        /// </param>
        /// <param name="retryQueueName">
        /// The retry queue's name.
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
            string retryQueueName,
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
        /// <param name="retryCount">
        /// The number of attempts made before this message was dead-lettered, recorded as
        /// <c>x-final-retry-count</c>. Passed explicitly by the caller rather than read back from the
        /// message's own headers, because under <see cref="RetryStrategy.InMemory"/> there is no
        /// <c>x-retry-count</c> header to read - retries never leave the consumer that is counting them.
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
            int retryCount,
            IChannel channel,
            CancellationToken cancellationToken);
    }
}
