using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitSharp.Abstractions;
using RabbitSharp.DependencyInjection;
using System.Collections.Concurrent;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// <c>AddRabbitSharp(name, ...)</c> against a real broker: two independently configured, named
    /// buses coexist on the same <see cref="IServiceProvider"/>, each resolved by its own key, each
    /// publishing and consuming on its own connection without interfering with the other.
    /// </summary>
    public class NamedBusTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public NamedBusTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record NamedBusOrdersEvent(Guid CorrelationId) : IMessage;

        private sealed record NamedBusBillingEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task TwoNamedBuses_PublishAndConsumeIndependently()
        {
            var services = new ServiceCollection();
            services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Information));

            services.AddRabbitSharp("orders", broker => _fixture.ConfigureBroker(broker, "named-bus-orders"));
            services.AddRabbitSharp("billing", broker => _fixture.ConfigureBroker(broker, "named-bus-billing"));

            await using var provider = services.BuildServiceProvider();

            var ordersBus = provider.GetRequiredKeyedService<IBus>("orders");
            var billingBus = provider.GetRequiredKeyedService<IBus>("billing");

            Assert.NotSame(ordersBus, billingBus);

            var ordersReceived = new ConcurrentBag<Guid>();
            var billingReceived = new ConcurrentBag<Guid>();

            await ordersBus.SubscribeAsync<NamedBusOrdersEvent>(
                "namedbus.orders.queue",
                msg => { ordersReceived.Add(msg.CorrelationId); return Task.CompletedTask; });

            await billingBus.SubscribeAsync<NamedBusBillingEvent>(
                "namedbus.billing.queue",
                msg => { billingReceived.Add(msg.CorrelationId); return Task.CompletedTask; });

            var ordersEvent = new NamedBusOrdersEvent(Guid.NewGuid());
            var billingEvent = new NamedBusBillingEvent(Guid.NewGuid());

            await ordersBus.PublishAsync(ordersEvent);
            await billingBus.PublishAsync(billingEvent);

            await Wait.UntilAsync(() => ordersReceived.Contains(ordersEvent.CorrelationId), "the orders bus delivers its own message");
            await Wait.UntilAsync(() => billingReceived.Contains(billingEvent.CorrelationId), "the billing bus delivers its own message");

            Assert.DoesNotContain(billingEvent.CorrelationId, ordersReceived);
            Assert.DoesNotContain(ordersEvent.CorrelationId, billingReceived);
        }
    }
}
