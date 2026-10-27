using RabbitMQ.Client;
using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using RabbitSharp.Options;
using System.Text.Json;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// A message that fails is republished to the retry or dead-letter queue, and that republish used
    /// to build fresh <c>BasicProperties</c> carrying only the message id, the timestamp and the
    /// headers. Everything else the producer set was dropped: a request/reply consumer lost the
    /// <c>reply-to</c> it needed to answer, a priority queue got everything back at priority 0, and
    /// the AMQP <c>correlation-id</c>, <c>content-type</c>, <c>type</c> and <c>app-id</c> a
    /// non-.NET producer relies on were gone after the first failure.
    /// </summary>
    public class PropertyPreservationTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public PropertyPreservationTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record RetryPropsEvent(Guid CorrelationId) : IMessage;
        private sealed record DeadLetterPropsEvent(Guid CorrelationId) : IMessage;

        private static readonly AmqpTimestamp ProducedAt = new(1700000000);

        [Fact]
        public async Task RepublishToTheRetryQueue_CarriesTheProducersProperties()
        {
            const string queueName = "props.retry.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.Strategy = RetryStrategy.RetryQueue;
                r.MaxDeliveryRetryAttempts = 1;
                // Long enough that the message is still parked in the retry queue to be inspected.
                r.InitialDeliveryRetryDelay = TimeSpan.FromSeconds(30);
            });

            await using var _ = services;
            await using var __ = bus;

            var attempts = 0;
            await bus.SubscribeAsync<RetryPropsEvent>(
                BuildOptions(queueName, "retrypropsevent", "RetryPropsEvent"),
                _ =>
                {
                    Interlocked.Increment(ref attempts);
                    throw new InvalidOperationException("always fails");
                });

            var message = new RetryPropsEvent(Guid.NewGuid());
            await PublishWithRichPropertiesAsync("retrypropsevent", "RetryPropsEvent", message, message.CorrelationId);

            await Wait.UntilAsync(
                async () => await _fixture.GetQueueDepthAsync($"{queueName}.retry") == 1,
                "the failed message is parked in the retry queue");

            var retried = await GetSingleAsync($"{queueName}.retry");

            AssertProducerPropertiesPreserved(retried.BasicProperties, message.CorrelationId);

            // The retry bookkeeping is still applied on top.
            Assert.Equal(1, Convert.ToInt32(retried.BasicProperties.Headers!["x-retry-count"]));
            Assert.Equal(1, Volatile.Read(ref attempts));
        }

        [Fact]
        public async Task RepublishToTheDeadLetterQueue_CarriesTheProducersProperties()
        {
            const string queueName = "props.deadletter.queue";

            var (services, bus) = BusTestHost.Create(
                _fixture,
                configureResilience: r => r.MaxDeliveryRetryAttempts = 0);

            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<DeadLetterPropsEvent>(
                BuildOptions(queueName, "deadletterpropsevent", "DeadLetterPropsEvent"),
                _ => throw new InvalidOperationException("always fails"));

            var message = new DeadLetterPropsEvent(Guid.NewGuid());
            await PublishWithRichPropertiesAsync("deadletterpropsevent", "DeadLetterPropsEvent", message, message.CorrelationId);

            await Wait.UntilAsync(
                async () => await _fixture.GetQueueDepthAsync($"{queueName}.deadletter") == 1,
                "the failed message reaches the dead-letter queue");

            var deadLettered = await GetSingleAsync($"{queueName}.deadletter");

            AssertProducerPropertiesPreserved(deadLettered.BasicProperties, message.CorrelationId);

            // The dead-letter bookkeeping is still applied on top, and the failure time lives in its
            // own header rather than overwriting the producer's timestamp.
            Assert.True(deadLettered.BasicProperties.Headers!.ContainsKey("x-failed-at"));
            Assert.True(deadLettered.BasicProperties.Headers!.ContainsKey("x-final-retry-count"));
            Assert.True(deadLettered.BasicProperties.Headers!.ContainsKey("x-original-queue"));
        }

        /// <summary>
        /// Two properties are dropped deliberately, and both would do damage if they were carried.
        /// </summary>
        [Fact]
        public async Task RepublishDropsExpirationAndUserId_WhichWouldBreakTheRepublish()
        {
            const string queueName = "props.excluded.queue";

            var (services, bus) = BusTestHost.Create(
                _fixture,
                configureResilience: r => r.MaxDeliveryRetryAttempts = 0);

            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<DeadLetterPropsEvent>(
                BuildOptions(queueName, "excludedpropsevent", "DeadLetterPropsEvent"),
                _ => throw new InvalidOperationException("always fails"));

            var message = new DeadLetterPropsEvent(Guid.NewGuid());

            await using (var connection = await _fixture.CreateRawConnectionAsync())
            await using (var channel = await connection.CreateChannelAsync())
            {
                await channel.BasicPublishAsync("excludedpropsevent", "DeadLetterPropsEvent", true,
                    new BasicProperties
                    {
                        MessageId = message.CorrelationId.ToString(),
                        Persistent = true,
                        // A per-message TTL: carried onto the dead-letter queue, which has no
                        // dead-letter exchange of its own, it would expire there and be dropped.
                        Expiration = "60000",
                        // Set to the producer's own authenticated user. The broker validates
                        // user-id against the publishing connection's user and answers a mismatch
                        // with 406 PRECONDITION_FAILED on basic.publish, which closes the channel -
                        // so carrying it would take the subscription down on the first failure of
                        // any message produced by a different user.
                        UserId = "rabbitmq"
                    },
                    JsonSerializer.SerializeToUtf8Bytes(message));
            }

            await Wait.UntilAsync(
                async () => await _fixture.GetQueueDepthAsync($"{queueName}.deadletter") == 1,
                "the failed message reaches the dead-letter queue");

            var deadLettered = await GetSingleAsync($"{queueName}.deadletter");

            Assert.Null(deadLettered.BasicProperties.Expiration);
            Assert.Null(deadLettered.BasicProperties.UserId);
        }

        private static RabbitSharp.Options.BusInfrastructureOptions BuildOptions(string queueName, string exchangeName, string routingKey)
        {
            return new BusInfrastructureBuilder(queueName)
                .WithExchange(e => e.WithName(exchangeName))
                .WithRoutingKey(routingKey)
                .WithBindingPattern($"{routingKey}.#")
                .Build();
        }

        private async Task PublishWithRichPropertiesAsync<T>(string exchangeName, string routingKey, T message, Guid messageId)
        {
            await using var connection = await _fixture.CreateRawConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            await channel.BasicPublishAsync(exchangeName, routingKey, mandatory: true,
                new BasicProperties
                {
                    MessageId = messageId.ToString(),
                    Persistent = true,
                    Timestamp = ProducedAt,
                    ContentType = "application/json",
                    ContentEncoding = "utf-8",
                    CorrelationId = "saga-4711",
                    ReplyTo = "orders.reply",
                    Type = "OrderCreated.v2",
                    AppId = "orders-api",
                    Priority = 5,
                    Headers = new Dictionary<string, object?> { ["x-tenant"] = "acme" }
                },
                JsonSerializer.SerializeToUtf8Bytes(message));
        }

        private static void AssertProducerPropertiesPreserved(IReadOnlyBasicProperties properties, Guid messageId)
        {
            Assert.Equal(messageId.ToString(), properties.MessageId);
            Assert.Equal("application/json", properties.ContentType);
            Assert.Equal("utf-8", properties.ContentEncoding);
            Assert.Equal("saga-4711", properties.CorrelationId);
            Assert.Equal("orders.reply", properties.ReplyTo);
            Assert.Equal("OrderCreated.v2", properties.Type);
            Assert.Equal("orders-api", properties.AppId);
            Assert.Equal((byte)5, properties.Priority);
            Assert.Equal(ProducedAt.UnixTime, properties.Timestamp.UnixTime);

            // Headers the producer set survive alongside the ones the library adds.
            Assert.Equal("acme", DecodeHeader(properties.Headers!["x-tenant"]));

            // Republished persistent regardless of the original delivery mode: a message on the
            // retry or dead-letter path has already failed once and must survive a broker restart.
            Assert.True(properties.Persistent);
        }

        private static string DecodeHeader(object? value) =>
            value is byte[] bytes ? System.Text.Encoding.UTF8.GetString(bytes) : value?.ToString() ?? string.Empty;

        private async Task<BasicGetResult> GetSingleAsync(string queueName)
        {
            await using var connection = await _fixture.CreateRawConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            var result = await channel.BasicGetAsync(queueName, autoAck: true);

            Assert.NotNull(result);
            return result!;
        }
    }
}
