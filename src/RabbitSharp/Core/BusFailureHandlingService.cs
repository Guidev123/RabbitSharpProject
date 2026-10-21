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
        private readonly ILogger<BusFailureHandlingService> _logger;
        private readonly INamingConventions _namingConventions;

        public BusFailureHandlingService(ILogger<BusFailureHandlingService> logger, INamingConventions namingConventions)
        {
            _logger = logger;
            _namingConventions = namingConventions;
        }

        public async Task DeclareInfrastructureAsync<T>(BusInfrastructureOptions options, IChannel channel, CancellationToken cancellationToken = default) where T : IMessage
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

            await DeclareQueueAsync(channel, options.MainQueue, cancellationToken);
            await DeclareQueueAsync(channel, options.DeadLetterQueue, cancellationToken);

            var retryArguments = new Dictionary<string, object?>(options.RetryQueue.Arguments)
            {
                ["x-dead-letter-exchange"] = string.Empty,
                ["x-dead-letter-routing-key"] = options.MainQueue.QueueName
            };

            await channel.QueueDeclareAsync(
                options.RetryQueue.QueueName,
                options.RetryQueue.Durable,
                options.RetryQueue.Exclusive,
                options.RetryQueue.AutoDelete,
                retryArguments,
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                options.MainQueue.QueueName,
                options.Exchange.ExchangeName,
                options.RoutingKey,
                cancellationToken: cancellationToken);

            LogInfrastructureDeclared(options.MainQueue.QueueName);
        }

        public async Task SendToRetryQueueAsync(
               BasicDeliverEventArgs eventArgs,
               string queueName,
               int retryCount,
               string correlationId,
               BusResilienceOptions busResilience,
               IChannel channel,
               CancellationToken cancellationToken)
        {
            var baseDelayMs = busResilience.InitialDeliveryRetryDelay.TotalMilliseconds;
            var exponentialDelayMs = baseDelayMs * Math.Pow(2, retryCount - 1);

            var maxDelayMs = busResilience.MaxDeliveryRetryDelay.TotalMilliseconds;
            var delayMs = (int)Math.Min(exponentialDelayMs, maxDelayMs);

            var originalHeaders = eventArgs.BasicProperties.Headers ?? new Dictionary<string, object?>();
            var headers = new Dictionary<string, object?>(originalHeaders)
            {
                ["x-retry-count"] = retryCount
            };

            var retryProperties = new BasicProperties
            {
                MessageId = eventArgs.BasicProperties.MessageId,
                Persistent = true,
                Timestamp = eventArgs.BasicProperties.Timestamp,
                Expiration = delayMs.ToString(),
                Headers = headers
            };

            await channel.BasicPublishAsync(string.Empty, queueName, true, retryProperties, eventArgs.Body, cancellationToken);

            LogSentToRetryQueue(correlationId, queueName, delayMs, maxDelayMs, retryCount);
        }

        public async Task SendToDeadLetterQueueAsync(
            BasicDeliverEventArgs eventArgs,
            string queueName,
            string correlationId,
            IChannel channel,
            CancellationToken cancellationToken)
        {
            var originalHeaders = eventArgs.BasicProperties.Headers ?? new Dictionary<string, object?>();
            var retryCount = GetRetryCount(eventArgs.BasicProperties);
            var headers = new Dictionary<string, object?>(originalHeaders)
            {
                ["x-original-queue"] = queueName.Replace(".deadletter", string.Empty),
                ["x-failed-at"] = DateTimeOffset.UtcNow.ToString("O"),
                ["x-final-retry-count"] = retryCount
            };

            var dlqProperties = new BasicProperties
            {
                MessageId = eventArgs.BasicProperties.MessageId,
                Persistent = true,
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                Headers = headers
            };

            await channel.BasicPublishAsync(string.Empty, queueName, true, dlqProperties, eventArgs.Body, cancellationToken);

            LogSentToDeadLetterQueue(correlationId, queueName, retryCount);
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

        [LoggerMessage(EventId = 102, Level = LogLevel.Information, Message = "Message {CorrelationId} sent to retry queue {RetryQueueName} with delay {DelayMs}ms (max: {MaxDelayMs}ms). Retry count: {RetryCount}")]
        private partial void LogSentToRetryQueue(string correlationId, string retryQueueName, int delayMs, double maxDelayMs, int retryCount);

        [LoggerMessage(EventId = 103, Level = LogLevel.Error, Message = "Message {CorrelationId} sent to dead letter queue {DeadLetterQueueName} after {RetryCount} failed attempts")]
        private partial void LogSentToDeadLetterQueue(string correlationId, string deadLetterQueueName, int retryCount);

        [LoggerMessage(EventId = 104, Level = LogLevel.Warning, Message = "Unrecognized x-retry-count header value of type {HeaderType}; treating retry count as 0 for this delivery")]
        private partial void LogUnrecognizedRetryCountHeader(string headerType);
    }
}
