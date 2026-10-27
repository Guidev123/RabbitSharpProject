using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitSharp.Abstractions;
using RabbitSharp.Extensions;
using RabbitSharp.Options;
using System.Text;

namespace RabbitSharp.Core
{
    internal sealed partial class BusFailureHandlingService : IBusFailureHandlingService
    {
        private const string DeadLetterExchangeArgument = "x-dead-letter-exchange";
        private const string DeadLetterRoutingKeyArgument = "x-dead-letter-routing-key";

        private readonly ILogger<BusFailureHandlingService> _logger;
        private readonly INamingConventions _namingConventions;

        public BusFailureHandlingService(ILogger<BusFailureHandlingService> logger, INamingConventions namingConventions)
        {
            _logger = logger;
            _namingConventions = namingConventions;
        }

        public async Task DeclareInfrastructureAsync<T>(
            BusInfrastructureOptions options,
            BusResilienceOptions busResilience,
            IChannel channel,
            CancellationToken cancellationToken = default) where T : IMessage
        {
            if (string.IsNullOrEmpty(options.Exchange.ExchangeName))
            {
                options.Exchange.ExchangeName = _namingConventions.GetExchangeName<T>();
            }

            if (string.IsNullOrEmpty(options.RoutingKey))
            {
                options.RoutingKey = _namingConventions.GetRoutingKey<T>(options.Exchange.Type);
            }

            await channel.ExchangeDeclareAsync(
                options.Exchange.ExchangeName,
                options.Exchange.Type.GetEnumDescription(),
                options.Exchange.Durable,
                arguments: options.Exchange.Arguments,
                cancellationToken: cancellationToken);

            if (!options.MainQueue.Arguments.ContainsKey(DeadLetterExchangeArgument))
            {
                options.MainQueue.Arguments[DeadLetterExchangeArgument] = string.Empty;
                options.MainQueue.Arguments[DeadLetterRoutingKeyArgument] = options.DeadLetterQueue.QueueName;
            }

            await DeclareQueueAsync(channel, options.MainQueue, cancellationToken);
            await DeclareQueueAsync(channel, options.DeadLetterQueue, cancellationToken);

            await DeclareRetryQueueAsync(channel, options, busResilience, cancellationToken);

            await channel.QueueBindAsync(
                options.MainQueue.QueueName,
                options.Exchange.ExchangeName,
                options.BindingPattern ?? options.RoutingKey,
                cancellationToken: cancellationToken);

            LogInfrastructureDeclared(options.MainQueue.QueueName);
        }

        /// <summary>
        /// Declares the single retry queue used by <see cref="RetryStrategy.RetryQueue"/>, or does
        /// nothing under <see cref="RetryStrategy.InMemory"/> or when no retries are configured.
        /// </summary>
        /// <remarks>
        /// The queue itself carries no <c>x-message-ttl</c>: the delay is set per message instead (see
        /// <see cref="SendToRetryQueueAsync"/>), so changing <see cref="BusResilienceOptions.InitialDeliveryRetryDelay"/>
        /// between deployments never redeclares this queue with a different fixed argument - which
        /// RabbitMQ would reject with <c>406 PRECONDITION_FAILED</c>. Every message published here
        /// carries the same delay, so the queue's FIFO order already matches expiry order and nothing
        /// is lost by collapsing what used to be one queue per distinct backoff delay into one.
        /// </remarks>
        private static async Task DeclareRetryQueueAsync(
            IChannel channel,
            BusInfrastructureOptions options,
            BusResilienceOptions busResilience,
            CancellationToken cancellationToken)
        {
            if (busResilience.Strategy != RetryStrategy.RetryQueue || busResilience.MaxDeliveryRetryAttempts == 0)
            {
                return;
            }

            var retryArguments = new Dictionary<string, object?>(options.RetryQueue.Arguments)
            {
                [DeadLetterExchangeArgument] = string.Empty,
                [DeadLetterRoutingKeyArgument] = options.MainQueue.QueueName
            };

            await channel.QueueDeclareAsync(
                options.RetryQueue.QueueName,
                options.RetryQueue.Durable,
                options.RetryQueue.Exclusive,
                options.RetryQueue.AutoDelete,
                retryArguments,
                cancellationToken: cancellationToken);
        }

        public async Task SendToRetryQueueAsync(
               BasicDeliverEventArgs eventArgs,
               string retryQueueName,
               int retryCount,
               string correlationId,
               BusResilienceOptions busResilience,
               IChannel channel,
               CancellationToken cancellationToken)
        {
            var delayMs = (int)busResilience.InitialDeliveryRetryDelay.TotalMilliseconds;

            var originalHeaders = eventArgs.BasicProperties.Headers ?? new Dictionary<string, object?>();
            var headers = new Dictionary<string, object?>(originalHeaders)
            {
                ["x-retry-count"] = retryCount
            };

            var retryProperties = BuildRepublishProperties(eventArgs.BasicProperties, headers, delayMs);

            await channel.BasicPublishAsync(string.Empty, retryQueueName, true, retryProperties, eventArgs.Body, cancellationToken);

            LogSentToRetryQueue(correlationId, retryQueueName, delayMs, retryCount);
        }

        public async Task SendToDeadLetterQueueAsync(
            BasicDeliverEventArgs eventArgs,
            string queueName,
            string correlationId,
            int retryCount,
            IChannel channel,
            CancellationToken cancellationToken)
        {
            var originalHeaders = eventArgs.BasicProperties.Headers ?? new Dictionary<string, object?>();
            var headers = new Dictionary<string, object?>(originalHeaders)
            {
                ["x-original-queue"] = queueName.Replace(".deadletter", string.Empty),
                ["x-failed-at"] = DateTimeOffset.UtcNow.ToString("O"),
                ["x-final-retry-count"] = retryCount
            };

            var dlqProperties = BuildRepublishProperties(eventArgs.BasicProperties, headers);

            await channel.BasicPublishAsync(string.Empty, queueName, true, dlqProperties, eventArgs.Body, cancellationToken);

            LogSentToDeadLetterQueue(correlationId, queueName, retryCount);
        }

        /// <summary>
        /// The properties for a republish to the retry or dead-letter queue: everything the producer
        /// set, with <paramref name="headers"/> replacing the original header table.
        /// </summary>
        /// <remarks>
        /// Built from RabbitMQ.Client's copy constructor rather than field by field, so a property
        /// this code does not know about is carried instead of silently dropped - which is how
        /// <c>content-type</c>, <c>correlation-id</c>, <c>reply-to</c>, <c>type</c>, <c>app-id</c>
        /// and <c>priority</c> used to disappear the first time a message failed, leaving a
        /// request/reply consumer with no <c>reply-to</c> to answer and a priority queue with
        /// everything back at priority 0.
        /// <para>
        /// Three properties are then overridden on purpose:
        /// </para>
        /// <list type="bullet">
        /// <item>
        /// <description>
        /// <c>Persistent</c> is forced on. A message on the retry or dead-letter path has already
        /// failed once; losing it to a broker restart because the producer published it transiently
        /// is worse than the cost of writing it to disk.
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// <c>Expiration</c> - the per-message TTL - is set to <paramref name="expirationMs"/> when
        /// given (a publish to the retry queue, which carries the whole backoff delay this way instead
        /// of as a fixed queue argument - see <see cref="DeclareRetryQueueAsync"/>), and cleared
        /// otherwise. A producer's own TTL is never carried through: on the retry queue it would
        /// compete with the delay this code sets, and on the dead-letter queue, which has no
        /// dead-letter exchange of its own, it would expire the message there and the broker would
        /// drop it.
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// <c>UserId</c> is cleared. The broker validates <c>user-id</c> against the authenticated
        /// user of the publishing connection and answers a mismatch with <c>406 PRECONDITION_FAILED
        /// - user_id property set to 'x' but authenticated user was 'y'</c> on
        /// <c>basic.publish</c>, which closes the channel. The connection republishing here belongs
        /// to the consumer, not the producer, so carrying it would take the subscription down on the
        /// first failure of any message produced by another service.
        /// </description>
        /// </item>
        /// </list>
        /// </remarks>
        internal static BasicProperties BuildRepublishProperties(
            IReadOnlyBasicProperties source,
            Dictionary<string, object?> headers,
            int? expirationMs = null)
        {
            return new BasicProperties(source)
            {
                Headers = headers,
                Persistent = true,
                Expiration = expirationMs?.ToString(),
                UserId = null
            };
        }

        public int GetRetryCount(IReadOnlyBasicProperties? properties)
        {
            if (properties?.Headers?.TryGetValue("x-retry-count", out var retryCountObj) != true || retryCountObj is null)
            {
                return 0;
            }

            switch (retryCountObj)
            {
                case int i: return i;
                case long l: return checked((int)Math.Clamp(l, 0, int.MaxValue));
                case short s: return s;
                case byte b: return b;
                case uint u: return (int)Math.Min(u, (uint)int.MaxValue);
                case string s when int.TryParse(s, out var parsedFromString):
                    return parsedFromString;

                case byte[] bytes when TryParseUtf8Int(bytes, out var parsedFromBytes):
                    return parsedFromBytes;

                default:
                    LogUnrecognizedRetryCountHeader(retryCountObj.GetType().Name);
                    return 0;
            }
        }

        private static bool TryParseUtf8Int(byte[] bytes, out int value)
        {
            return int.TryParse(Encoding.UTF8.GetString(bytes), out value);
        }

        private static async Task DeclareQueueAsync(IChannel channel, QueueOptions options, CancellationToken cancellationToken)
        {
            await channel.QueueDeclareAsync(
                options.QueueName,
                options.Durable,
                options.Exclusive,
                options.AutoDelete,
                options.Arguments,
                cancellationToken: cancellationToken);
        }

        [LoggerMessage(EventId = 101, Level = LogLevel.Information, Message = "Infrastructure declared for {QueueName} with custom options")]
        private partial void LogInfrastructureDeclared(string queueName);

        [LoggerMessage(EventId = 102, Level = LogLevel.Information, Message = "Message {CorrelationId} sent to retry queue {RetryQueueName} with delay {DelayMs}ms. Retry count: {RetryCount}")]
        private partial void LogSentToRetryQueue(string correlationId, string retryQueueName, int delayMs, int retryCount);

        [LoggerMessage(EventId = 103, Level = LogLevel.Error, Message = "Message {CorrelationId} sent to dead letter queue {DeadLetterQueueName} after {RetryCount} failed attempts")]
        private partial void LogSentToDeadLetterQueue(string correlationId, string deadLetterQueueName, int retryCount);

        [LoggerMessage(EventId = 104, Level = LogLevel.Warning, Message = "Unrecognized x-retry-count header value of type {HeaderType}; treating retry count as 0 for this delivery")]
        private partial void LogUnrecognizedRetryCountHeader(string headerType);
    }
}
