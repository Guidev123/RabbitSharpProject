using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// <c>ForDevelopment</c> is meant to make a dev environment tidy up after itself, which is a
    /// reasonable thing to want of the main queue - it has a consumer, so it only counts as unused
    /// once the application is gone. A retry queue is different: it never has a consumer and is never
    /// redeclared after startup, so "unused" is its steady state and <c>x-expires</c> deletes it
    /// while the subscription is perfectly healthy. Retries then stop happening, and a message that
    /// happens to be waiting out its TTL in there goes with the queue.
    /// </summary>
    public class DevelopmentPresetTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public DevelopmentPresetTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record DevPresetEvent(Guid CorrelationId) : IMessage;

        // Its own type, so the convention-based PublishAsync resolves to this test's own exchange
        // rather than one another test in this class already bound a queue to.
        private sealed record DevPresetRetryEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task ForDevelopment_ExpiresTheMainQueueButNeverTheRetryQueues()
        {
            const string queueName = "devpreset.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.MaxDeliveryRetryAttempts = 2;
                r.InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1);
                r.MaxDeliveryRetryDelay = TimeSpan.FromMinutes(5);
            });

            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<DevPresetEvent>(
                new BusInfrastructureBuilder(queueName)
                    .WithExchange(e => e.WithName("devpresetevent"))
                    .WithRoutingKey("DevPresetEvent")
                    .WithBindingPattern("DevPresetEvent.#")
                    .ForDevelopment()
                    .Build(),
                _ => Task.CompletedTask);

            // The preset still does its documented job on the queue that can meaningfully be unused.
            var mainArguments = await _fixture.GetQueueArgumentsAsync(queueName);
            Assert.True(mainArguments.ContainsKey("x-expires"), "the main queue should still expire when unused.");

            // The regression: every retry queue was declared with the same x-expires, so the retry
            // path quietly deleted itself after half an hour of a healthy, idle subscription.
            foreach (var delayMs in new[] { 1000, 2000 })
            {
                var retryQueueName = $"{queueName}.retry.{delayMs}ms";
                var retryArguments = await _fixture.GetQueueArgumentsAsync(retryQueueName);

                Assert.True(retryArguments.Count > 0, $"Expected retry queue {retryQueueName} to exist.");
                Assert.False(
                    retryArguments.ContainsKey("x-expires"),
                    $"Retry queue {retryQueueName} must not expire: nothing ever consumes from it or " +
                    $"redeclares it, so it would be deleted while the subscription is still healthy.");
            }
        }

        [Fact]
        public async Task ForDevelopment_StillRetriesAFailingMessage()
        {
            const string queueName = "devpreset.retrying.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.MaxDeliveryRetryAttempts = 2;
                r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(300);
                r.MaxDeliveryRetryDelay = TimeSpan.FromSeconds(1);
            });

            await using var _ = services;
            await using var __ = bus;

            var attempts = 0;

            await bus.SubscribeAsync<DevPresetRetryEvent>(
                new BusInfrastructureBuilder(queueName)
                    .WithExchange(e => e.WithName("devpresetretryevent"))
                    .WithRoutingKey("DevPresetRetryEvent")
                    .WithBindingPattern("DevPresetRetryEvent.#")
                    .ForDevelopment()
                    .Build(),
                _ =>
                {
                    Interlocked.Increment(ref attempts);
                    throw new InvalidOperationException("always fails");
                });

            await bus.PublishAsync(new DevPresetRetryEvent(Guid.NewGuid()));

            await Wait.UntilAsync(
                async () => await _fixture.GetQueueDepthAsync($"{queueName}.deadletter") == 1,
                "the exhausted message reaches its dead-letter queue");

            // Initial delivery plus both configured retries - the retry queues were really used.
            Assert.Equal(3, Volatile.Read(ref attempts));
        }
    }
}
