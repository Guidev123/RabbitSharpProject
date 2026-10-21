using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using RabbitSharp.Enums;
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

        private readonly ConcurrentDictionary<string, byte> _declaredExchanges = new();

        private IChannel? _lastPublishChannel;

        private readonly List<(IChannel Channel, string ConsumerTag)> _activeConsumers = new();
        private readonly object _activeConsumersLock = new();
        private bool _disposed;

        public Bus(IBusFailureHandlingService busFailureHandlingService,
                   IConnectionManager connectionManager,
                   ILogger<Bus> logger,
                   INamingConventions namingConventions,
                   Action<BusResilienceOptions>? configureResilience = null)
        {
            _busResilienceOptions = new BusResilienceOptions();
            configureResilience?.Invoke(_busResilienceOptions);
            _busResilienceOptions.Validate();

            _busFailureHandlingService = busFailureHandlingService;
            _logger = logger;
            _namingConventions = namingConventions;
            _connectionManager = connectionManager;
        }

        public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : IMessage
        {
            var options = new PublisherOptionsBuilder()
                .WithExchange(exchange =>
                {
                    exchange.WithName(_namingConventions.GetExchangeName<T>());
                    exchange.WithType(ExchangeTypeEnum.Topic);
                })
                .WithRoutingKey(_namingConventions.GetRoutingKey<T>(ExchangeTypeEnum.Topic))
                .Build();

            return PublishAsync(options, message, cancellationToken);
        }

        public async Task PublishAsync<T>(PublisherOptions options, T message, CancellationToken cancellationToken = default) where T : IMessage
        {
            var channel = await _connectionManager.GetPublishChannelAsync(cancellationToken);

            if (!ReferenceEquals(channel, _lastPublishChannel))
            {
                _declaredExchanges.Clear();
                _lastPublishChannel = channel;
            }

            var exchangeName = options.Exchange.ExchangeName!;
            var exchangeCacheKey = $"{exchangeName}|{options.Exchange.Type}|{options.Exchange.Durable}";

            if (_declaredExchanges.TryAdd(exchangeCacheKey, 0))
            {
                await channel.ExchangeDeclareAsync(
                    exchangeName,
                    options.Exchange.Type.GetEnumDescription(),
                    options.Exchange.Durable,
                    arguments: options.Exchange.Arguments,
                    cancellationToken: cancellationToken);
            }

            var body = JsonSerializer.SerializeToUtf8Bytes(message);
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
            var options = new BusInfrastructureBuilder(queueName)
                .WithExchange(exchange =>
                {
                    exchange.WithType(ExchangeTypeEnum.Topic);
                    exchange.WithName(_namingConventions.GetExchangeName<T>());
                })
                .WithRoutingKey(_namingConventions.GetRoutingKey<T>(ExchangeTypeEnum.Topic))
                .Build();

            await SubscribeAsync(options, onMessage, cancellationToken);
        }

        public async Task SubscribeAsync<T>(BusInfrastructureOptions options, Func<T, Task> onMessage, CancellationToken cancellationToken = default) where T : IMessage
        {
            var channel = await _connectionManager.CreateConsumerChannelAsync(cancellationToken);

            await _busFailureHandlingService.DeclareInfrastructureAsync<T>(options, channel, cancellationToken);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (model, eventArgs) =>
            {
                var deliveryTag = eventArgs.DeliveryTag;
                var correlationId = eventArgs.BasicProperties?.MessageId ?? string.Empty;

                try
                {
                    var message = JsonSerializer.Deserialize<T>(eventArgs.Body.Span);

                    if (message is null)
                    {
                        LogDeserializeFailed(correlationId, options.MainQueue.QueueName);
                        await channel.BasicAckAsync(deliveryTag, false);
                        return;
                    }

                    LogReceived(typeof(T).Name, message.CorrelationId, options.MainQueue.QueueName);

                    await onMessage(message);

                    await channel.BasicAckAsync(deliveryTag, false);

                    LogProcessed(typeof(T).Name, message.CorrelationId, options.MainQueue.QueueName);
                }
                catch (Exception ex)
                {
                    LogProcessingError(ex, correlationId, options.MainQueue.QueueName);

                    var retryCount = _busFailureHandlingService.GetRetryCount(eventArgs.BasicProperties);

                    try
                    {
                        if (retryCount < _busResilienceOptions.MaxDeliveryRetryAttempts)
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
                            LogRetriesExceeded(correlationId, options.MainQueue.QueueName, _busResilienceOptions.MaxDeliveryRetryAttempts);

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
                        LogFailureHandlingError(handlingEx, correlationId, options.MainQueue.QueueName);

                        try
                        {
                            await channel.BasicNackAsync(deliveryTag, false, true);
                        }
                        catch
                        {
                        }
                    }
                }
            };

            var consumerTag = await channel.BasicConsumeAsync(options.MainQueue.QueueName, false, consumer, cancellationToken: cancellationToken);

            RegisterActiveConsumer(channel, consumerTag, cancellationToken);
        }

        private void RegisterActiveConsumer(IChannel channel, string consumerTag, CancellationToken cancellationToken)
        {
            lock (_activeConsumersLock)
            {
                _activeConsumers.Add((channel, consumerTag));
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

            List<(IChannel Channel, string ConsumerTag)> consumers;
            lock (_activeConsumersLock)
            {
                consumers = new List<(IChannel, string)>(_activeConsumers);
                _activeConsumers.Clear();
            }

            foreach (var (channel, consumerTag) in consumers)
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

        [LoggerMessage(EventId = 4, Level = LogLevel.Error, Message = "Failed to deserialize message {CorrelationId} from queue {QueueName}")]
        private partial void LogDeserializeFailed(string correlationId, string queueName);

        [LoggerMessage(EventId = 5, Level = LogLevel.Error, Message = "Error processing message {CorrelationId} from queue {QueueName}")]
        private partial void LogProcessingError(Exception ex, string correlationId, string queueName);

        [LoggerMessage(EventId = 6, Level = LogLevel.Warning, Message = "Message {CorrelationId} sent to retry queue. Attempt {RetryCount}/{MaxDeliveryRetryAttempts}")]
        private partial void LogSentToRetry(string correlationId, int retryCount, int maxDeliveryRetryAttempts);

        [LoggerMessage(EventId = 7, Level = LogLevel.Error, Message = "Message {CorrelationId} from queue {QueueName} exceeded maximum retries ({MaxDeliveryRetryAttempts})")]
        private partial void LogRetriesExceeded(string correlationId, string queueName, int maxDeliveryRetryAttempts);

        [LoggerMessage(EventId = 8, Level = LogLevel.Error, Message = "Failed to send message {CorrelationId} to the retry/dead-letter queue for {QueueName}; nacking with requeue")]
        private partial void LogFailureHandlingError(Exception ex, string correlationId, string queueName);
    }
}
