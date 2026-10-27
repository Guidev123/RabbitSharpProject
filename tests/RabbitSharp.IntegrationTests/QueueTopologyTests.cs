using RabbitSharp.Abstractions;
using RabbitSharp.Options;
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
        public async Task DefaultStrategy_InMemory_DeclaresNoRetryQueueAtAll()
        {
            const string queueName = "topology.inmemory.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.MaxDeliveryRetryAttempts = 3;
                r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(500);
                r.MaxDeliveryRetryDelay = TimeSpan.FromSeconds(30);
            });

            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<TopologyEvent>(queueName, _ => Task.CompletedTask);

            Assert.False(
                await _fixture.QueueExistsAsync($"{queueName}.retry"),
                "RetryStrategy.InMemory, the default, must not declare a retry queue.");
        }

        [Fact]
        public async Task RetryQueueStrategy_DeclaresASingleRetryQueueWithNoFixedTtl()
        {
            const string queueName = "topology.retry.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.Strategy = RetryStrategy.RetryQueue;
                r.MaxDeliveryRetryAttempts = 3;
                r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(500);
            });

            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<TopologyEvent>(queueName, _ => Task.CompletedTask);

            var retryQueueName = $"{queueName}.retry";
            var arguments = await _fixture.GetQueueArgumentsAsync(retryQueueName);

            Assert.True(arguments.Count > 0, $"Expected retry queue {retryQueueName} to exist.");
            // No fixed x-message-ttl: the delay is carried per message instead, so the delay can
            // change between deployments without a 406 PRECONDITION_FAILED on redeclare.
            Assert.False(arguments.ContainsKey("x-message-ttl"));
            Assert.Equal(string.Empty, arguments["x-dead-letter-exchange"]);
            Assert.Equal(queueName, arguments["x-dead-letter-routing-key"]);
        }
    }
}
