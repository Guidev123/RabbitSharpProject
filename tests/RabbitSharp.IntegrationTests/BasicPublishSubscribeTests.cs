using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using RabbitSharp.Enums;
using System.Collections.Concurrent;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    public class BasicPublishSubscribeTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public BasicPublishSubscribeTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record OrderCreated(Guid CorrelationId, string CustomerName) : IMessage;

        [Fact]
        public async Task ConventionBasedSubscribeAsync_ReceivesPublishedMessage()
        {
            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var received = new ConcurrentBag<OrderCreated>();
            await bus.SubscribeAsync<OrderCreated>("basic.orders.queue",
                msg => { received.Add(msg); return Task.CompletedTask; });

            var order = new OrderCreated(Guid.NewGuid(), "Ada Lovelace");
            await bus.PublishAsync(order);

            await Wait.UntilAsync(
                () => received.Any(m => m.CorrelationId == order.CorrelationId),
                "the published order is handled");

            var match = Assert.Single(received, m => m.CorrelationId == order.CorrelationId);
            Assert.Equal(order.CustomerName, match.CustomerName);
        }

        [Fact]
        public async Task CustomPublisherOptions_DirectExchange_RoutesToMatchingQueueOnly()
        {
            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var received = new ConcurrentBag<Guid>();

            await bus.SubscribeAsync<OrderCreated>(
                new BusInfrastructureBuilder("basic.direct.queue")
                    .WithExchange(e =>
                    {
                        e.WithName("orders.direct");
                        e.WithType(ExchangeTypeEnum.Direct);
                    })
                    .WithRoutingKey("order-created")
                    .Build(),
                msg => { received.Add(msg.CorrelationId); return Task.CompletedTask; });

            var order = new OrderCreated(Guid.NewGuid(), "Grace Hopper");
            var publisherOptions = new PublisherOptionsBuilder()
                .WithExchange(e =>
                {
                    e.WithName("orders.direct");
                    e.WithType(ExchangeTypeEnum.Direct);
                })
                .WithRoutingKey("order-created")
                .Build();

            await bus.PublishAsync(publisherOptions, order);

            await Wait.UntilAsync(
                () => received.Contains(order.CorrelationId),
                "the direct-exchange message is handled");
        }
    }
}
