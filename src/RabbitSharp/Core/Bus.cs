using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using RabbitSharp.Enums;
using RabbitSharp.Extensions;
using RabbitSharp.Options;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
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

        /// <summary>
        /// Which exchanges have been declared, per channel they were declared on.
        /// </summary>
        /// <remarks>
        /// Keyed by the channel rather than tracked against a "last channel seen" field, so there is
        /// no cross-channel state to invalidate and no window in which one publisher clears the cache
        /// while another is deciding whether to declare. A channel that goes away takes its entry with
        /// it: the table holds its key weakly, so a replaced channel is not kept alive by this and its
        /// entry does not accumulate.
        /// </remarks>
        private readonly ConditionalWeakTable<IChannel, ConcurrentDictionary<ExchangeDeclaration, byte>> _declaredExchanges = new();

        /// <summary>
        /// The publisher configuration the convention-based overload resolves for a message type.
        /// Cached because the naming conventions and the configured exchange type are fixed for the
        /// life of the bus, so rebuilding it per message only allocated a builder, an options graph
        /// and a freshly lowercased exchange name on every publish.
        /// </summary>
        private readonly ConcurrentDictionary<Type, PublisherOptions> _conventionPublisherOptions = new();

        private readonly List<ActiveConsumer> _activeConsumers = new();
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

            return PublishAsync(GetConventionPublisherOptions<T>(), message, cancellationToken);
        }

        /// <summary>
        /// The publisher configuration for <typeparamref name="T"/>, resolved from the naming
        /// conventions once and reused afterwards.
        /// </summary>
        /// <remarks>
        /// This makes the conventions a function of the message type alone, consulted once per type
        /// instead of once per message - which is what they already were in practice: they are handed
        /// nothing but a <see cref="Type"/>, and the subscribe path has always asked them exactly once
        /// per subscription.
        /// </remarks>
        private PublisherOptions GetConventionPublisherOptions<T>() where T : IMessage
        {
            if (_conventionPublisherOptions.TryGetValue(typeof(T), out var cached))
            {
                return cached;
            }

            var exchangeType = _busMessagingOptions.DefaultExchangeType;

            var options = new PublisherOptionsBuilder()
                .WithExchange(exchange =>
                {
                    exchange.WithName(_namingConventions.GetExchangeName<T>());
                    exchange.WithType(exchangeType);
                })
                .WithRoutingKey(_namingConventions.GetRoutingKey<T>(exchangeType))
                .Build();

            return _conventionPublisherOptions.GetOrAdd(typeof(T), options);
        }

        public async Task PublishAsync<T>(PublisherOptions options, T message, CancellationToken cancellationToken = default) where T : IMessage
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(message);

            var channel = await _connectionManager.GetPublishChannelAsync(cancellationToken);

            var exchangeName = options.Exchange.ExchangeName!;
            var declaration = new ExchangeDeclaration(exchangeName, options.Exchange.Type, options.Exchange.Durable);

            var declaredOnThisChannel = _declaredExchanges.GetValue(
                channel,
                static _ => new ConcurrentDictionary<ExchangeDeclaration, byte>());

            if (!declaredOnThisChannel.ContainsKey(declaration))
            {
                await channel.ExchangeDeclareAsync(
                    exchangeName,
                    options.Exchange.Type.GetEnumDescription(),
                    options.Exchange.Durable,
                    arguments: options.Exchange.Arguments,
                    cancellationToken: cancellationToken);

                declaredOnThisChannel.TryAdd(declaration, 0);
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
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
            ArgumentNullException.ThrowIfNull(onMessage);

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

            options.Validate(_busResilienceOptions);

            var channel = await _connectionManager.CreateConsumerChannelAsync(cancellationToken);

            try
            {
                await _busFailureHandlingService.DeclareInfrastructureAsync<T>(options, _busResilienceOptions, channel, cancellationToken);

                var queueName = options.MainQueue.QueueName;
                var consumer = new AsyncEventingBasicConsumer(channel);

                consumer.ReceivedAsync += (model, eventArgs) =>
                    HandleDeliveryAsync(eventArgs, options, channel, onMessage, cancellationToken);

                consumer.RegisteredAsync += (model, eventArgs) =>
                    HandleConsumerRegisteredAsync(channel, queueName, eventArgs.ConsumerTags);

                consumer.UnregisteredAsync += (model, eventArgs) =>
                    channel.IsOpen
                        ? HandleSubscriptionInterruptionAsync(
                            channel,
                            queueName,
                            eventArgs.ConsumerTags.FirstOrDefault(),
                            SubscriptionInterruptionReason.ConsumerCancelledByBroker)
                        : Task.CompletedTask;

                consumer.ShutdownAsync += (model, shutdownArgs) =>
                    HandleConsumerShutdownAsync(channel, queueName, shutdownArgs);

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

            if (message is null)
            {
                LogDeserializeFailed(correlationId, queueName);
                await HandleFailedDeliveryAsync(eventArgs, options, channel, correlationId, isPoisonPayload: true, cancellationToken);
                return;
            }

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
                if (!isPoisonPayload && retryCount < _busResilienceOptions.MaxDeliveryRetryAttempts)
                {
                    try
                    {
                        await _busFailureHandlingService.SendToRetryQueueAsync(
                            eventArgs,
                            options.RetryQueue.QueueName,
                            retryCount + 1,
                            correlationId,
                            _busResilienceOptions,
                            channel,
                            cancellationToken);
                    }
                    catch (Exception retryEx)
                    {
                        LogRetryQueueUnavailable(retryEx, correlationId, options.RetryQueue.QueueName);

                        await DeadLetterAndAckAsync(eventArgs, options, channel, correlationId, cancellationToken);
                        return;
                    }

                    await channel.BasicAckAsync(deliveryTag, false);

                    LogSentToRetry(correlationId, retryCount + 1, _busResilienceOptions.MaxDeliveryRetryAttempts);
                }
                else
                {
                    if (!isPoisonPayload)
                    {
                        LogRetriesExceeded(correlationId, queueName, _busResilienceOptions.MaxDeliveryRetryAttempts);
                    }

                    await DeadLetterAndAckAsync(eventArgs, options, channel, correlationId, cancellationToken);
                }
            }
            catch (Exception handlingEx)
            {
                LogFailureHandlingError(handlingEx, correlationId, queueName);

                await HoldBackAndStopSubscriptionAsync(channel, deliveryTag, correlationId, queueName);
            }
        }

        private async Task DeadLetterAndAckAsync(
            BasicDeliverEventArgs eventArgs,
            BusInfrastructureOptions options,
            IChannel channel,
            string correlationId,
            CancellationToken cancellationToken)
        {
            await _busFailureHandlingService.SendToDeadLetterQueueAsync(
                eventArgs,
                options.DeadLetterQueue.QueueName,
                correlationId,
                channel,
                cancellationToken);

            await channel.BasicAckAsync(eventArgs.DeliveryTag, false);
        }

        /// <summary>
        /// The last resort when neither the retry queue nor the dead-letter queue would take the
        /// message: leave it on the main queue and stop consuming.
        /// </summary>
        /// <remarks>
        /// Requeued, not nacked without requeue - which is what this used to do. Nacking without
        /// requeue dead-letters the message through the main queue's dead-letter exchange, into the
        /// same dead-letter queue that just rejected it, and RabbitMQ silently discards a
        /// dead-lettered message that a <c>reject-publish</c> queue refuses. The message was
        /// destroyed with nothing in any queue to replay. Requeueing keeps it; the subscription then
        /// has to stop, because a requeued message comes straight back for the same failure and
        /// nothing would ever break that loop.
        /// </remarks>
        private async Task HoldBackAndStopSubscriptionAsync(
            IChannel channel,
            ulong deliveryTag,
            string correlationId,
            string queueName)
        {
            try
            {
                await channel.BasicNackAsync(deliveryTag, false, true);
            }
            catch (Exception nackEx)
            {
                LogNackFailed(nackEx, correlationId, queueName);
            }

            _ = Task.Run(() => StopSubscriptionAsync(channel, queueName));
        }

        private async Task StopSubscriptionAsync(IChannel channel, string queueName)
        {
            ActiveConsumer consumer;
            lock (_activeConsumersLock)
            {
                var index = _activeConsumers.FindIndex(c => ReferenceEquals(c.Channel, channel));

                if (index < 0)
                {
                    return;
                }

                consumer = _activeConsumers[index];
                _activeConsumers.RemoveAt(index);
            }

            var consumerTag = consumer.ConsumerTag;

            LogSubscriptionStoppedWithNowhereToPutFailures(queueName);

            await RaiseSubscriptionInterruptedAsync(
                new SubscriptionInterruptedEventArgs(
                    queueName,
                    consumerTag,
                    SubscriptionInterruptionReason.FailureHandlingUnavailable),
                queueName);

            await StopConsumerAsync(consumer);
        }

        /// <summary>
        /// Whether a channel shutdown is one RabbitMQ.Client's automatic recovery undoes by itself.
        /// </summary>
        /// <remarks>
        /// A connection-level close takes every channel down with it and is followed by recovery
        /// re-registering this very consumer, so there is nothing to report and nothing to
        /// deregister. A channel-level error - a 406 from mismatched queue arguments, a 403, a 404 -
        /// closes only this channel and is never recovered, so that one is a real interruption.
        /// </remarks>
        private static bool IsRecoverableShutdown(ShutdownEventArgs shutdownArgs)
        {
            return shutdownArgs.Initiator == ShutdownInitiator.Library
                || shutdownArgs.ReplyCode == Constants.ConnectionForced;
        }

        private Task HandleConsumerShutdownAsync(IChannel channel, string queueName, ShutdownEventArgs shutdownArgs)
        {
            if (shutdownArgs.ReplyCode == Constants.ReplySuccess)
            {
                return Task.CompletedTask;
            }

            if (IsRecoverableShutdown(shutdownArgs))
            {
                LogSubscriptionInterruptedByConnectionLoss(queueName, shutdownArgs.ReplyCode, shutdownArgs.ReplyText ?? string.Empty);
                return Task.CompletedTask;
            }

            return HandleSubscriptionInterruptionAsync(
                channel,
                queueName,
                consumerTag: null,
                SubscriptionInterruptionReason.ChannelShutdown,
                shutdownArgs.ReplyCode,
                shutdownArgs.ReplyText);
        }

        /// <summary>
        /// Keeps the registered consumer tag in step with what is actually running. Automatic
        /// recovery re-registers the consumer and may hand it a new tag; the registration has to
        /// follow, or <see cref="DisposeAsync"/> cancels a tag the broker no longer knows.
        /// </summary>
        private Task HandleConsumerRegisteredAsync(IChannel channel, string queueName, IReadOnlyList<string> consumerTags)
        {
            var consumerTag = consumerTags.Count > 0 ? consumerTags[0] : null;
            if (consumerTag is null)
            {
                return Task.CompletedTask;
            }

            lock (_activeConsumersLock)
            {
                var index = _activeConsumers.FindIndex(c => ReferenceEquals(c.Channel, channel));
                if (index < 0)
                {
                    return Task.CompletedTask;
                }

                _activeConsumers[index].ConsumerTag = consumerTag;
            }

            LogSubscriptionRecovered(queueName, consumerTag);
            return Task.CompletedTask;
        }

        private async Task HandleSubscriptionInterruptionAsync(
            IChannel channel,
            string queueName,
            string? consumerTag,
            SubscriptionInterruptionReason reason,
            int? replyCode = null,
            string? replyText = null)
        {
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

            await RaiseSubscriptionInterruptedAsync(
                new SubscriptionInterruptedEventArgs(queueName, consumerTag, reason, replyCode, replyText),
                queueName);
        }

        private async Task RaiseSubscriptionInterruptedAsync(SubscriptionInterruptedEventArgs args, string queueName)
        {
            var handler = SubscriptionInterrupted;
            if (handler is null)
            {
                return;
            }

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
            var consumer = new ActiveConsumer(channel, consumerTag, queueName);

            lock (_activeConsumersLock)
            {
                _activeConsumers.Add(consumer);
            }

            if (!cancellationToken.CanBeCanceled)
            {
                return;
            }

            var registration = cancellationToken.Register(static state =>
            {
                var (self, channel) = ((Bus, IChannel))state!;
                _ = self.StopRegisteredConsumerAsync(channel);
            }, (this, channel));

            var stillRegistered = false;
            lock (_activeConsumersLock)
            {
                if (_activeConsumers.Contains(consumer))
                {
                    consumer.CancellationRegistration = registration;
                    stillRegistered = true;
                }
            }

            if (!stillRegistered)
            {
                registration.Unregister();
            }
        }

        private async Task StopRegisteredConsumerAsync(IChannel channel)
        {
            ActiveConsumer consumer;
            lock (_activeConsumersLock)
            {
                var index = _activeConsumers.FindIndex(c => ReferenceEquals(c.Channel, channel));
                if (index < 0)
                {
                    return;
                }

                consumer = _activeConsumers[index];
                _activeConsumers.RemoveAt(index);
            }

            await StopConsumerAsync(consumer);
        }

        private int RemoveActiveConsumers(IChannel channel)
        {
            List<ActiveConsumer> removed;
            lock (_activeConsumersLock)
            {
                removed = _activeConsumers.FindAll(c => ReferenceEquals(c.Channel, channel));
                _activeConsumers.RemoveAll(c => ReferenceEquals(c.Channel, channel));
            }

            foreach (var consumer in removed)
            {
                consumer.ReleaseCancellationRegistration();
            }

            return removed.Count;
        }

        private async Task StopConsumerAsync(ActiveConsumer consumer)
        {
            consumer.ReleaseCancellationRegistration();

            try
            {
                if (consumer.Channel.IsOpen)
                {
                    await consumer.Channel.BasicCancelAsync(consumer.ConsumerTag);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to cancel consumer {ConsumerTag} cleanly.", consumer.ConsumerTag);
            }

            await SafeCloseAsync(consumer.Channel);
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

            List<ActiveConsumer> consumers;
            lock (_activeConsumersLock)
            {
                consumers = new List<ActiveConsumer>(_activeConsumers);
                _activeConsumers.Clear();
            }

            foreach (var consumer in consumers)
            {
                await StopConsumerAsync(consumer);
            }
        }

        /// <summary>
        /// What makes one exchange declaration different from another, as far as the broker is
        /// concerned: redeclaring the same name with a different type or durability is rejected with
        /// <c>406 PRECONDITION_FAILED</c>, so all three belong in the key.
        /// </summary>
        private readonly record struct ExchangeDeclaration(string Name, ExchangeTypeEnum Type, bool Durable);

        /// <summary>
        /// One live subscription: the channel it owns, the consumer tag the broker currently knows it
        /// by - which automatic recovery can replace - and the registration that stops it when its
        /// cancellation token fires.
        /// </summary>
        private sealed class ActiveConsumer
        {
            public ActiveConsumer(IChannel channel, string consumerTag, string queueName)
            {
                Channel = channel;
                ConsumerTag = consumerTag;
                QueueName = queueName;
            }

            public IChannel Channel { get; }

            public string ConsumerTag { get; set; }

            public string QueueName { get; }

            public CancellationTokenRegistration CancellationRegistration { get; set; }

            /// <summary>
            /// Releases the cancellation callback once this subscription has stopped.
            /// </summary>
            /// <remarks>
            /// The token a subscription is given is typically the application's lifetime token, which
            /// outlives every subscription made with it. Left registered, it holds the callback - and
            /// through the callback's state, the bus and this channel - for the rest of the process,
            /// one leaked pair per subscription.
            /// <para>
            /// <c>Unregister</c> rather than <c>Dispose</c>: this can run from inside that very
            /// callback (cancellation is what stopped the subscription), and <c>Dispose</c> waits for
            /// a running callback to finish.
            /// </para>
            /// </remarks>
            public void ReleaseCancellationRegistration()
            {
                CancellationRegistration.Unregister();
                CancellationRegistration = default;
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

        [LoggerMessage(EventId = 8, Level = LogLevel.Critical, Message = "Failed to send message {CorrelationId} to the dead-letter queue for {QueueName}; it was requeued on the main queue and this subscription has been stopped, because nacking it without requeue would dead-letter it into the same queue that just rejected it and the broker would discard it")]
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

        [LoggerMessage(EventId = 15, Level = LogLevel.Warning, Message = "The subscription on queue {QueueName} lost its connection ({ReplyCode} {ReplyText}); automatic recovery will restore it, so it is still registered and no interruption is reported")]
        private partial void LogSubscriptionInterruptedByConnectionLoss(string queueName, int replyCode, string replyText);

        [LoggerMessage(EventId = 16, Level = LogLevel.Information, Message = "The subscription on queue {QueueName} was restored by automatic recovery and is consuming again as {ConsumerTag}")]
        private partial void LogSubscriptionRecovered(string queueName, string consumerTag);

        [LoggerMessage(EventId = 17, Level = LogLevel.Error, Message = "Retry queue {RetryQueueName} would not take message {CorrelationId}; dead-lettering it instead of retrying it")]
        private partial void LogRetryQueueUnavailable(Exception ex, string correlationId, string retryQueueName);

        [LoggerMessage(EventId = 18, Level = LogLevel.Critical, Message = "The subscription on queue {QueueName} has been stopped: a failed message could reach neither the retry queue nor the dead-letter queue, and it was left on the main queue rather than destroyed. Drain or raise the limit of the dead-letter queue, then re-subscribe")]
        private partial void LogSubscriptionStoppedWithNowhereToPutFailures(string queueName);
    }
}
