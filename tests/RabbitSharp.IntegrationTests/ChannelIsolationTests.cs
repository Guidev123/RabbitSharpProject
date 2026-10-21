using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using RabbitSharp.Enums;
using System.Collections.Concurrent;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    public class ChannelIsolationTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public ChannelIsolationTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record IsolationTestEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task A406OnOneSubscription_DoesNotAffectPublishingOrOtherSubscriptions()
        {
            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var received = new ConcurrentBag<Guid>();

            await bus.SubscribeAsync<IsolationTestEvent>("isolation.queue",
                msg => { received.Add(msg.CorrelationId); return Task.CompletedTask; });

            // Redeclares the SAME queue with a conflicting x-max-length argument, forcing the broker
            // to reject with 406 PRECONDITION_FAILED. This closes only this SubscribeAsync call's
            // own channel.
            await Assert.ThrowsAnyAsync<Exception>(() => bus.SubscribeAsync<IsolationTestEvent>(
                new BusInfrastructureBuilder("isolation.queue")
                    .WithExchange(e =>
                    {
                        e.WithName("isolationtestevent");
                        e.WithType(ExchangeTypeEnum.Topic);
                    })
                    .WithRoutingKey("IsolationTestEvent")
                    .WithBindingPattern("IsolationTestEvent.#")
                    .WithMainQueue(q => q.WithMaxLength(123))
                    .Build(),
                _ => Task.CompletedTask));

            var survivorEvent = new IsolationTestEvent(Guid.NewGuid());
            var publishException = await Record.ExceptionAsync(() => bus.PublishAsync(survivorEvent));

            Assert.Null(publishException);

            await Wait.UntilAsync(
                () => received.Contains(survivorEvent.CorrelationId),
                "the surviving subscription still handles messages");
        }
    }
}
