using RabbitMQ.Client;
using RabbitSharp.Abstractions;
using System.Collections.Concurrent;
using System.Text;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// Payloads from producers that are not this library: a camelCase body must bind, and a body that
    /// cannot be read must end up in the dead-letter queue rather than being acked and discarded.
    /// </summary>
    public class PayloadInteropTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public PayloadInteropTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record InteropEvent(Guid CorrelationId, string CustomerName) : IMessage;

        private async Task PublishRawAsync(string routingKey, string body)
        {
            await using var connection = await _fixture.CreateRawConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            await channel.BasicPublishAsync(
                exchange: "interopevent",
                routingKey: routingKey,
                mandatory: false,
                basicProperties: new BasicProperties { MessageId = Guid.NewGuid().ToString() },
                body: Encoding.UTF8.GetBytes(body));
        }

        [Fact]
        public async Task CamelCasePayloadFromAnotherProducer_BindsInsteadOfArrivingBlank()
        {
            const string queueName = "interop.camelcase.queue";

            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var received = new ConcurrentBag<InteropEvent>();
            await bus.SubscribeAsync<InteropEvent>(queueName, msg => { received.Add(msg); return Task.CompletedTask; });

            var correlationId = Guid.NewGuid();
            await PublishRawAsync("InteropEvent", $"{{\"correlationId\":\"{correlationId}\",\"customerName\":\"Grace\"}}");

            await Wait.UntilAsync(() => received.Count == 1, "the camelCase payload is handled");

            var message = received.Single();
            Assert.Equal(correlationId, message.CorrelationId);
            Assert.Equal("Grace", message.CustomerName);
        }

        [Theory]
        [InlineData("null")]
        [InlineData("{not json at all")]
        [InlineData("{\"customerName\":\"no correlation id here\"}")]
        public async Task UnreadablePayload_ReachesTheDeadLetterQueueAndNeverTheHandler(string body)
        {
            var queueName = $"interop.unreadable.{Math.Abs(body.GetHashCode())}.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.MaxDeliveryRetryAttempts = 3;
                r.InitialDeliveryRetryDelay = TimeSpan.FromSeconds(10);
                r.MaxDeliveryRetryDelay = TimeSpan.FromSeconds(30);
            });

            await using var _ = services;
            await using var __ = bus;

            var handlerCalls = 0;
            await bus.SubscribeAsync<InteropEvent>(queueName, _ =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            });

            await PublishRawAsync("InteropEvent", body);

            // Straight to the dead-letter queue: the retry delay here is 10s, so anything arriving
            // within a couple of seconds did not go through the retry queue.
            await Wait.UntilAsync(
                async () => await _fixture.GetQueueDepthAsync($"{queueName}.deadletter") == 1,
                "the unreadable payload reaches the dead-letter queue");

            Assert.Equal(0, Volatile.Read(ref handlerCalls));
        }
    }
}
