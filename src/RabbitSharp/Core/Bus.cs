using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using RabbitSharp.Extensions;
using RabbitSharp.Options;
using System.Collections.Concurrent;
using System.Text.Json;

namespace RabbitSharp.Core
{
    internal sealed partial class Bus : IBus
    {
        private readonly IBusFailureHandlingService _busFailureHandlingService;
        private readonly IConnectionManager _connectionManager;
        private readonly INamingConventions _namingConventions;
        private readonly ILogger<Bus> _logger;
        private readonly BusResilienceOptions _busResilienceOptions;
        private readonly BusMessagingOptions _busMessagingOptions;

        private readonly ConcurrentDictionary<string, byte> _declaredExchanges = new();

        private IChannel? _lastPublishChannel;

        private readonly List<(IChannel Channel, string ConsumerTag, string QueueName)> _activeConsumers = new();
        private readonly object _activeConsumersLock = new();
        private bool _disposed;

        public Bus(IBusFailureHandlingService busFailureHandlingService,
                   IConnectionManager connectionManager,
                   ILogger<Bus> logger,
                   INamingConventions namingConventions,
                   Action<BusResilienceOptions>? configureResilience = null,
                   Action<BusMessagingOptions>? configureMessaging = null)
        {
            _busResilienceOptions = new BusResilienceOptions();
            configureResilience?.Invoke(_busResilienceOptions);
            _busResilienceOptions.Validate();

            _busMessagingOptions = new BusMessagingOptions();
            configureMessaging?.Invoke(_busMessagingOptions);
            _busMessagingOptions.Validate();

            _busFailureHandlingService = busFailureHandlingService;
            _logger = logger;
            _namingConventions = namingConventions;
            _connectionManager = connectionManager;
        }

        public event Func<SubscriptionInterruptedEventArgs, Task>? SubscriptionInterrupted;

        public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : IMessage
        {
            ArgumentNullException.ThrowIfNull(message);

            var exchangeType = _busMessagingOptions.DefaultExchangeType;

            var options = new PublisherOptionsBuilder()
                .WithExchange(exchange =>
                {
                    exchange.WithName(_namingConventions.GetExchangeName<T>());
                    exchange.WithType(exchangeType);
                })
                .WithRoutingKey(_namingConventions.GetRoutingKey<T>(exchangeType))
                .Build();

            return PublishAsync(options, message, cancellationToken);
        }

        public async Task PublishAsync<T>(PublisherOptions options, T message, CancellationToken cancellationToken = default) where T : IMessage
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(message);

            var channel = await _connectionManager.GetPublishChannelAsync(cancellationToken);

            if (!ReferenceEquals(channel, _lastPublishChannel))
            {
                _declaredExchanges.Clear();
                _lastPublishChannel = channel;
            }

            var exchangeName = options.Exchange.ExchangeName!;
            var exchangeCacheKey = $"{exchangeName}|{options.Exchange.Type}|{options.Exchange.Durable}";

            // Declared before the cache is marked, never after: marking first leaves a window in which
            // another publisher skips the declare and publishes to an exchange that does not exist yet.
            if (!_declaredExchanges.ContainsKey(exchangeCacheKey))
            {
                await channel.ExchangeDeclareAsync(
                    exchangeName,
                    options.Exchange.Type.GetEnumDescription(),
                    options.Exchange.Durable,
                    arguments: options.Exchange.Arguments,
                    cancellationToken: cancellationToken);

                _declaredExchanges.TryAdd(exchangeCacheKey, 0);
            }

            var body = JsonSerializer.SerializeToUtf8Bytes(message, _busMessagingOptions.SerializerOptions);
            var properties = new BasicProperties
            {
                MessageId = message.CorrelationId.ToString(),
                Persistent = true,
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                Headers = options.Headers
            };

            await channel.BasicPublishAsync(exchangeName, options.RoutingKey!, true, properties, body, cancellationToken);

            LogPublished(typeof(T).Name, message.CorrelationId, exchangeName);
        }

        public async Task SubscribeAsync<T>(string queueName, Func<T, Task> onMessage, CancellationToken cancellationToken = default) where T : IMessage
        {
            var exchangeType = _busMessagingOptions.DefaultExchangeType;

            var options = new BusInfrastructureBuilder(queueName)
                .WithExchange(exchange =>
                {
                    exchange.WithType(exchangeType);
                    exchange.WithName(_namingConventions.GetExchangeName<T>());
                })
                .WithRoutingKey(_namingConventions.GetRoutingKey<T>(exchangeType))
                .WithBindingPattern(_namingConventions.GetBindingPattern<T>(exchangeType))
                .Build();

            await SubscribeAsync(options, onMessage, cancellationToken);
        }

        public async Task SubscribeAsync<T>(BusInfrastructureOptions options, Func<T, Task> onMessage, CancellationToken cancellationToken = default) where T : IMessage
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(onMessage);

            var channel = await _connectionManager.CreateConsumerChannelAsync(cancellationToken);

            // Everything from here on runs against a channel nothing else holds a reference to yet, so
            // any failure in between has to close it - otherwise each failed or cancelled
            // SubscribeAsync leaks one open channel until the connection runs out of them.
            try
            {
                await _busFailureHandlingService.DeclareInfrastructureAsync<T>(options, _busResilienceOptions, channel, cancellationToken);

                var queueName = options.MainQueue.QueueName;
                var consumer = new AsyncEventingBasicConsumer(channel);

                consumer.ReceivedAsync += (model, eventArgs) =>
                    HandleDeliveryAsync(eventArgs, options, channel, onMessage, cancellationToken);

                consumer.UnregisteredAsync += (model, eventArgs) =>
                    HandleSubscriptionInterruptionAsync(
                        channel,
                        queueName,
                        eventArgs.ConsumerTags.FirstOrDefault(),
                        SubscriptionInterruptionReason.ConsumerCancelledByBroker);

                consumer.ShutdownAsync += (model, shutdownArgs) =>
                    shutdownArgs.ReplyCode == Constants.ReplySuccess
                        ? Task.CompletedTask
                        : HandleSubscriptionInterruptionAsync(
                            channel,
                            queueName,
                            consumerTag: null,
                            SubscriptionInterruptionReason.ChannelShutdown,
                            shutdownArgs.ReplyCode,
                            shutdownArgs.ReplyText);

                var consumerTag = await channel.BasicConsumeAsync(queueName, false, consumer, cancellationToken: cancellationToken);

                RegisterActiveConsumer(channel, consumerTag, queueName, cancellationToken);
            }
            catch
            {
                await SafeCloseAsync(channel);
                throw;
            }
        }

        private async Task HandleDeliveryAsync<T>(
            BasicDeliverEventArgs eventArgs,
            BusInfrastructureOptions options,
            IChannel channel,
            Func<T, Task> onMessage,
            CancellationToken cancellationToken) where T : IMessage
        {
            var deliveryTag = eventArgs.DeliveryTag;
            var queueName = options.MainQueue.QueueName;
            var correlationId = eventArgs.BasicProperties?.MessageId ?? string.Empty;

            T? message;
            try
            {
                message = JsonSerializer.Deserialize<T>(eventArgs.Body.Span, _busMessagingOptions.SerializerOptions);
            }
            catch (Exception ex)
            {
                LogDeserializeError(ex, correlationId, queueName);
                await HandleFailedDeliveryAsync(eventArgs, options, channel, correlationId, isPoisonPayload: true, cancellationToken);
                return;
            }

            // A null body deserializes to null without throwing. Acking it here - as this used to -
            // destroys the message with nothing in the dead-letter queue and nothing to replay.
            if (message is null)
            {
                LogDeserializeFailed(correlationId, queueName);
                await HandleFailedDeliveryAsync(eventArgs, options, channel, correlationId, isPoisonPayload: true, cancellationToken);
                return;
            }

            // An all-defaults instance is what a payload whose property names did not bind looks like,
            // and a record with a parameterized constructor never throws for the missing properties.
            if (_busMessagingOptions.RejectMessagesWithEmptyCorrelationId && message.CorrelationId == Guid.Empty)
            {
                LogEmptyCorrelationId(correlationId, queueName);
                await HandleFailedDeliveryAsync(eventArgs, options, channel, correlationId, isPoisonPayload: true, cancellationToken);
                return;
            }

            LogReceived(typeof(T).Name, message.CorrelationId, queueName);

            try
            {
                await onMessage(message);
            }
            catch (Exception ex)
            {
                LogProcessingError(ex, correlationId, queueName);
                await HandleFailedDeliveryAsync(eventArgs, options, channel, correlationId, isPoisonPayload: false, cancellationToken);
                return;
            }

            // The ack gets its own try: a connection drop between the handler returning and the ack
            // landing is an infrastructure failure, not a handler failure. Retrying or dead-lettering
            // here would duplicate the business effect of work that already succeeded, and logging it
            // as a processing error would blame the handler for it.
            try
            {
                await channel.BasicAckAsync(deliveryTag, false);
            }
            catch (Exception ex)
            {
                LogAckFailed(ex, correlationId, queueName);
                return;
            }

            LogProcessed(typeof(T).Name, message.CorrelationId, queueName);
        }

        private async Task HandleFailedDeliveryAsync(
            BasicDeliverEventArgs eventArgs,
            BusInfrastructureOptions options,
            IChannel channel,
            string correlationId,
            bool isPoisonPayload,
            CancellationToken cancellationToken)
        {
            var deliveryTag = eventArgs.DeliveryTag;
            var queueName = options.MainQueue.QueueName;
            var retryCount = _busFailureHandlingService.GetRetryCount(eventArgs.BasicProperties);

            try
            {
                // A payload that cannot be read will not become readable on the next attempt, so it
                // goes straight to the dead-letter queue instead of burning the retry budget.
                if (!isPoisonPayload && retryCount < _busResilienceOptions.MaxDeliveryRetryAttempts)
                {
                    await _busFailureHandlingService.SendToRetryQueueAsync(
                        eventArgs,
                        options.RetryQueue.QueueName,
                        retryCount + 1,
                        correlationId,
                        _busResilienceOptions,
                        channel,
                        cancellationToken);

                    await channel.BasicAckAsync(deliveryTag, false);

                    LogSentToRetry(correlationId, retryCount + 1, _busResilienceOptions.MaxDeliveryRetryAttempts);
                }
                else
                {
                    if (!isPoisonPayload)
                    {
                        LogRetriesExceeded(correlationId, queueName, _busResilienceOptions.MaxDeliveryRetryAttempts);
                    }

                    await _busFailureHandlingService.SendToDeadLetterQueueAsync(
                        eventArgs,
                        options.DeadLetterQueue.QueueName,
                        correlationId,
                        channel,
                        cancellationToken);

                    await channel.BasicAckAsync(deliveryTag, false);
                }
            }
            catch (Exception handlingEx)
            {
                LogFailureHandlingError(handlingEx, correlationId, queueName);

                try
                {
                    // Nacked without requeue on purpose: requeueing puts the message straight back for
                    // the same failure, with no backoff, and - when the retry queue is what rejected
                    // the publish - that loop never ends. Without requeue the main queue's dead-letter
                    // exchange takes it.
                    await channel.BasicNackAsync(deliveryTag, false, false);
                }
                catch (Exception nackEx)
                {
                    LogNackFailed(nackEx, correlationId, queueName);
                }
            }
        }

        private async Task HandleSubscriptionInterruptionAsync(
            IChannel channel,
            string queueName,
            string? consumerTag,
            SubscriptionInterruptionReason reason,
            int? replyCode = null,
            string? replyText = null)
        {
            // Nothing left to deregister means this subscription was already stopped on purpose - by
            // its cancellation token or by DisposeAsync - so the cancel and the channel shutdown that
            // follow are expected, not an interruption to report.
            if (RemoveActiveConsumers(channel) == 0)
            {
                return;
            }

            if (reason == SubscriptionInterruptionReason.ConsumerCancelledByBroker)
            {
                LogConsumerCancelledByBroker(queueName, consumerTag ?? string.Empty);
            }
            else
            {
                LogSubscriptionChannelShutdown(queueName, replyCode ?? 0, replyText ?? string.Empty);
            }

            var handler = SubscriptionInterrupted;
            if (handler is null)
            {
                return;
            }

            var args = new SubscriptionInterruptedEventArgs(queueName, consumerTag, reason, replyCode, replyText);

            foreach (var subscriber in handler.GetInvocationList().Cast<Func<SubscriptionInterruptedEventArgs, Task>>())
            {
                try
                {
                    await subscriber(args);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "A SubscriptionInterrupted handler threw for queue {QueueName}.", queueName);
                }
            }
        }

        private void RegisterActiveConsumer(IChannel channel, string consumerTag, string queueName, CancellationToken cancellationToken)
        {
            lock (_activeConsumersLock)
            {
                _activeConsumers.Add((channel, consumerTag, queueName));
            }

            if (cancellationToken.CanBeCanceled)
            {
                cancellationToken.Register(static state =>
                {
                    var (self, channel, consumerTag) = ((Bus, IChannel, string))state!;
                    _ = self.StopConsumerAsync(channel, consumerTag);
                }, (this, channel, consumerTag));
            }
        }

        private int RemoveActiveConsumers(IChannel channel)
        {
            lock (_activeConsumersLock)
            {
                return _activeConsumers.RemoveAll(c => ReferenceEquals(c.Channel, channel));
            }
        }

        private async Task StopConsumerAsync(IChannel channel, string consumerTag)
        {
            lock (_activeConsumersLock)
            {
                _activeConsumers.RemoveAll(c => c.ConsumerTag == consumerTag);
            }

            try
            {
                if (channel.IsOpen)
                {
                    await channel.BasicCancelAsync(consumerTag);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to cancel consumer {ConsumerTag} cleanly.", consumerTag);
            }

            await SafeCloseAsync(channel);
        }

        private static async Task SafeCloseAsync(IChannel channel)
        {
            try
            {
                if (channel.IsOpen)
                {
                    await channel.CloseAsync();
                }
            }
            catch
            {
            }

            try
            {
                await channel.DisposeAsync();
            }
            catch
            {
            }
        }

        public void Dispose()
        {
            DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            List<(IChannel Channel, string ConsumerTag, string QueueName)> consumers;
            lock (_activeConsumersLock)
            {
                consumers = new List<(IChannel, string, string)>(_activeConsumers);
                _activeConsumers.Clear();
            }

            foreach (var (channel, consumerTag, _) in consumers)
            {
                await StopConsumerAsync(channel, consumerTag);
            }
        }

        [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Published message {MessageType} with ID {CorrelationId} to exchange {ExchangeName}.")]
        private partial void LogPublished(string messageType, Guid correlationId, string exchangeName);

        [LoggerMessage(EventId = 2, Level = LogLevel.Debug, Message = "Received message {MessageType} with ID {CorrelationId} from queue {QueueName}")]
        private partial void LogReceived(string messageType, Guid correlationId, string queueName);

        [LoggerMessage(EventId = 3, Level = LogLevel.Debug, Message = "Processed message {MessageType} with ID {CorrelationId} from queue {QueueName}")]
        private partial void LogProcessed(string messageType, Guid correlationId, string queueName);

        [LoggerMessage(EventId = 4, Level = LogLevel.Error, Message = "Message {CorrelationId} from queue {QueueName} deserialized to null; dead-lettering it instead of discarding it")]
        private partial void LogDeserializeFailed(string correlationId, string queueName);

        [LoggerMessage(EventId = 5, Level = LogLevel.Error, Message = "Error processing message {CorrelationId} from queue {QueueName}")]
        private partial void LogProcessingError(Exception ex, string correlationId, string queueName);

        [LoggerMessage(EventId = 6, Level = LogLevel.Warning, Message = "Message {CorrelationId} sent to retry queue. Attempt {RetryCount}/{MaxDeliveryRetryAttempts}")]
        private partial void LogSentToRetry(string correlationId, int retryCount, int maxDeliveryRetryAttempts);

        [LoggerMessage(EventId = 7, Level = LogLevel.Error, Message = "Message {CorrelationId} from queue {QueueName} exceeded maximum retries ({MaxDeliveryRetryAttempts})")]
        private partial void LogRetriesExceeded(string correlationId, string queueName, int maxDeliveryRetryAttempts);

        [LoggerMessage(EventId = 8, Level = LogLevel.Error, Message = "Failed to send message {CorrelationId} to the retry/dead-letter queue for {QueueName}; nacking without requeue so the main queue dead-letter exchange takes it")]
        private partial void LogFailureHandlingError(Exception ex, string correlationId, string queueName);

        [LoggerMessage(EventId = 9, Level = LogLevel.Error, Message = "Message {CorrelationId} from queue {QueueName} was handled successfully but could not be acknowledged; the broker will redeliver it. It was NOT sent to retry or dead-letter, so the handler must tolerate a redelivery")]
        private partial void LogAckFailed(Exception ex, string correlationId, string queueName);

        [LoggerMessage(EventId = 10, Level = LogLevel.Error, Message = "Failed to deserialize message {CorrelationId} from queue {QueueName}; dead-lettering it")]
        private partial void LogDeserializeError(Exception ex, string correlationId, string queueName);

        [LoggerMessage(EventId = 11, Level = LogLevel.Error, Message = "Message {CorrelationId} from queue {QueueName} deserialized with an empty CorrelationId, which means its property names did not bind; dead-lettering it instead of handing an all-defaults instance to the handler")]
        private partial void LogEmptyCorrelationId(string correlationId, string queueName);

        [LoggerMessage(EventId = 12, Level = LogLevel.Error, Message = "The broker cancelled the consumer {ConsumerTag} on queue {QueueName}; this subscription has stopped consuming and will not recover on its own")]
        private partial void LogConsumerCancelledByBroker(string queueName, string consumerTag);

        [LoggerMessage(EventId = 13, Level = LogLevel.Error, Message = "The channel of the subscription on queue {QueueName} shut down ({ReplyCode} {ReplyText}); this subscription has stopped consuming")]
        private partial void LogSubscriptionChannelShutdown(string queueName, int replyCode, string replyText);

        [LoggerMessage(EventId = 14, Level = LogLevel.Error, Message = "Failed to nack message {CorrelationId} from queue {QueueName} after its failure handling failed; the broker will redeliver it when the channel closes")]
        private partial void LogNackFailed(Exception ex, string correlationId, string queueName);
    }
}
