using RabbitSharp.Abstractions;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// Asserts, against the broker, what the default topology actually is - the arguments a queue was
    /// declared with decide whether a full or expired message is captured or destroyed.
    /// </summary>
    public class QueueTopologyTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public QueueTopologyTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record TopologyEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task DefaultTopology_NoQueueDropsItsOwnHead_AndTheMainQueueDeadLettersToTheDlq()
        {
            const string queueName = "topology.main.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.MaxDeliveryRetryAttempts = 3;
                r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(500);
                r.MaxDeliveryRetryDelay = TimeSpan.FromSeconds(30);
            });

            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<TopologyEvent>(queueName, _ => Task.CompletedTask);

            var mainArguments = await _fixture.GetQueueArgumentsAsync(queueName);
            var deadLetterArguments = await _fixture.GetQueueArgumentsAsync($"{queueName}.deadletter");

            // A full queue must reject the publish, not quietly destroy its oldest message while the
            // publisher is told the publish succeeded.
            Assert.Equal("reject-publish", mainArguments["x-overflow"]);
            Assert.Equal("reject-publish", deadLetterArguments["x-overflow"]);

            // Without this, overflow / TTL expiry / rejection on the main queue goes nowhere at all.
            Assert.Equal(string.Empty, mainArguments["x-dead-letter-exchange"]);
            Assert.Equal($"{queueName}.deadletter", mainArguments["x-dead-letter-routing-key"]);
        }

        [Fact]
        public async Task RetryQueues_OnePerBackoffDelay_EachWithItsOwnFixedTtl()
        {
            const string queueName = "topology.retry.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.MaxDeliveryRetryAttempts = 3;
                r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(500);
                r.MaxDeliveryRetryDelay = TimeSpan.FromSeconds(30);
            });

            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<TopologyEvent>(queueName, _ => Task.CompletedTask);

            // 500ms, 1000ms, 2000ms - one queue each, so a long wait cannot hold up a short one behind
            // it (RabbitMQ only evaluates a per-message TTL at the head of the queue).
            foreach (var delayMs in new[] { 500, 1000, 2000 })
            {
                var retryQueueName = $"{queueName}.retry.{delayMs}ms";
                var arguments = await _fixture.GetQueueArgumentsAsync(retryQueueName);

                Assert.True(arguments.Count > 0, $"Expected retry queue {retryQueueName} to exist.");
                Assert.Equal(delayMs.ToString(), arguments["x-message-ttl"]);
                Assert.Equal(string.Empty, arguments["x-dead-letter-exchange"]);
                Assert.Equal(queueName, arguments["x-dead-letter-routing-key"]);
            }
        }
    }
}
