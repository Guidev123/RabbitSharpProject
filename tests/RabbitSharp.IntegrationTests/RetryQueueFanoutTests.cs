using RabbitSharp.Abstractions;
using RabbitSharp.Options;
using System.Collections.Concurrent;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    public class RetryQueueFanoutTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public RetryQueueFanoutTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record FanoutTestEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task FailingConsumer_RetryDoesNotFanOutToOtherSubscribersOfSameTopic()
        {
            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.Strategy = RetryStrategy.RetryQueue;
                r.MaxDeliveryRetryAttempts = 1;
                r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(200);
            });

            await using var _ = services;
            await using var __ = bus;

            var svcBReceived = new ConcurrentBag<Guid>();
            var svcAAttempts = new ConcurrentBag<Guid>();

            await bus.SubscribeAsync<FanoutTestEvent>("fanout.svcB.queue",
                msg => { svcBReceived.Add(msg.CorrelationId); return Task.CompletedTask; });

            await bus.SubscribeAsync<FanoutTestEvent>("fanout.svcA.queue", msg =>
            {
                svcAAttempts.Add(msg.CorrelationId);
                throw new InvalidOperationException("svcA always fails (simulated downstream failure)");
            });

            var testEvent = new FanoutTestEvent(Guid.NewGuid());
            await bus.PublishAsync(testEvent);

            await Wait.UntilAsync(
                () => svcAAttempts.Count(id => id == testEvent.CorrelationId) == 2,
                "the failing subscriber has had its one retry");

            // svcA's retry must not reach svcB: it is republished to svcA's own retry queue, not back
            // through the shared exchange.
            await Wait.StaysFalseAsync(
                () => svcBReceived.Count(id => id == testEvent.CorrelationId) > 1,
                "the other subscriber receiving the retry");

            Assert.Equal(1, svcBReceived.Count(id => id == testEvent.CorrelationId));
            Assert.Equal(2, svcAAttempts.Count(id => id == testEvent.CorrelationId));
        }

        [Fact]
        public async Task FailingConsumer_MessageEventuallyReachesItsOwnDeadLetterQueueWithRetryMetadata()
        {
            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.Strategy = RetryStrategy.RetryQueue;
                r.MaxDeliveryRetryAttempts = 1;
                r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(200);
            });

            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<FanoutTestEvent>("fanout.dlq.queue", _ =>
                throw new InvalidOperationException("always fails"));

            var testEvent = new FanoutTestEvent(Guid.NewGuid());
            await bus.PublishAsync(testEvent);

            await Wait.UntilAsync(
                async () => await _fixture.GetQueueDepthAsync("fanout.dlq.queue.deadletter") == 1,
                "the exhausted message reaches its dead-letter queue");

            await using var rawConnection = await _fixture.CreateRawConnectionAsync();
            await using var rawChannel = await rawConnection.CreateChannelAsync();

            var result = await rawChannel.BasicGetAsync("fanout.dlq.queue.deadletter", autoAck: true);

            Assert.NotNull(result);
            Assert.True(result!.BasicProperties.Headers?.ContainsKey("x-final-retry-count"));
            Assert.True(result.BasicProperties.Headers?.ContainsKey("x-original-queue"));
        }
    }
}
