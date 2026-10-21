using RabbitMQ.Client.Exceptions;
using RabbitSharp.Abstractions;
using System.Collections.Concurrent;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    public class PublisherConfirmationTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public PublisherConfirmationTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record BoundEvent(Guid CorrelationId) : IMessage;
        private sealed record UnboundEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task PublishingToExchangeWithNoBoundQueue_Throws()
        {
            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            await Assert.ThrowsAsync<PublishException>(
                () => bus.PublishAsync(new UnboundEvent(Guid.NewGuid())));
        }

        [Fact]
        public async Task PublishingToARoutableQueue_SucceedsAndIsReceived()
        {
            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var received = new ConcurrentBag<Guid>();
            await bus.SubscribeAsync<BoundEvent>("confirmations.bound.queue",
                msg => { received.Add(msg.CorrelationId); return Task.CompletedTask; });

            var testEvent = new BoundEvent(Guid.NewGuid());
            var exception = await Record.ExceptionAsync(() => bus.PublishAsync(testEvent));

            Assert.Null(exception);

            await Wait.UntilAsync(
                () => received.Contains(testEvent.CorrelationId),
                "the confirmed message is handled");
        }
    }
}
