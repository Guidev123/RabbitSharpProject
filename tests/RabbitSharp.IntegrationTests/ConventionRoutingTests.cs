using RabbitMQ.Client;
using RabbitSharp.Abstractions;
using System.Collections.Concurrent;
using System.Text.Json;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// The convention publishes a concrete routing key and binds with a pattern. Both halves have to
    /// work: the library's own publish must reach its own subscriber, and so must the sub-keys a
    /// conventional producer uses.
    /// </summary>
    public class ConventionRoutingTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public ConventionRoutingTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record RoutingEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task ConventionBoundQueue_ReceivesBothTheLibraryKeyAndAConventionalSubKey()
        {
            const string queueName = "routing.convention.queue";

            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var received = new ConcurrentBag<Guid>();
            await bus.SubscribeAsync<RoutingEvent>(queueName, msg => { received.Add(msg.CorrelationId); return Task.CompletedTask; });

            // Published by the library itself, with the concrete key "RoutingEvent".
            var fromLibrary = new RoutingEvent(Guid.NewGuid());
            await bus.PublishAsync(fromLibrary);

            // Published the way another service would, with a sub-key under the same type.
            var fromOtherProducer = Guid.NewGuid();
            await using (var connection = await _fixture.CreateRawConnectionAsync())
            await using (var channel = await connection.CreateChannelAsync())
            {
                await channel.BasicPublishAsync(
                    exchange: "routingevent",
                    routingKey: "RoutingEvent.created",
                    mandatory: false,
                    basicProperties: new BasicProperties { MessageId = fromOtherProducer.ToString() },
                    body: JsonSerializer.SerializeToUtf8Bytes(new { CorrelationId = fromOtherProducer }));
            }

            await Wait.UntilAsync(() => received.Count == 2, "both routing keys are delivered");

            Assert.Contains(fromLibrary.CorrelationId, received);
            Assert.Contains(fromOtherProducer, received);
        }

        [Fact]
        public async Task ConventionPublish_UsesAConcreteRoutingKeyRatherThanABindingPattern()
        {
            const string queueName = "routing.concrete.queue";

            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<RoutingEvent>(queueName, _ => Task.CompletedTask);

            // A queue bound with a key a sensible consumer would use. If the library published the
            // wildcard pattern "RoutingEvent.#" as its routing key, nothing would arrive here.
            await using var connection = await _fixture.CreateRawConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();
            await channel.QueueDeclareAsync("routing.exact.queue", durable: false, exclusive: false, autoDelete: true);
            await channel.QueueBindAsync("routing.exact.queue", "routingevent", "RoutingEvent");

            await bus.PublishAsync(new RoutingEvent(Guid.NewGuid()));

            await Wait.UntilAsync(
                async () => await channel.MessageCountAsync("routing.exact.queue") == 1,
                "the exactly bound queue receives the published key");
        }
    }
}
