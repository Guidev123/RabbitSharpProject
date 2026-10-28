using System.Collections.Concurrent;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// A message type does not need to implement <c>RabbitSharp.Abstractions.IMessage</c> any more -
    /// only to expose a public <c>Guid CorrelationId</c> property, which is read by convention at
    /// runtime. This is what a project that only defines message contracts, with no reference to
    /// RabbitSharp at all, would actually write.
    /// </summary>
    public class MessageContractDuckTypingTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public MessageContractDuckTypingTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        // Deliberately does not implement IMessage, and has no RabbitSharp.Abstractions using at all.
        private sealed record PlainContractEvent(Guid CorrelationId, string CustomerName);

        [Fact]
        public async Task PublishAndSubscribe_RoundTripsAPlainTypeThatDoesNotImplementIMessage()
        {
            const string queueName = "ducktyping.plain.queue";

            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var received = new ConcurrentBag<PlainContractEvent>();
            await bus.SubscribeAsync<PlainContractEvent>(
                queueName,
                msg => { received.Add(msg); return Task.CompletedTask; });

            var published = new PlainContractEvent(Guid.NewGuid(), "Grace");
            await bus.PublishAsync(published);

            await Wait.UntilAsync(() => received.Count == 1, "the plain, non-IMessage payload is handled");

            var message = received.Single();
            Assert.Equal(published.CorrelationId, message.CorrelationId);
            Assert.Equal("Grace", message.CustomerName);
        }
    }
}
