using RabbitSharp.Abstractions;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// <see cref="RabbitSharp.Options.RetryStrategy.InMemory"/> is the default: a failed delivery is
    /// retried in-process, with no retry queue ever declared or published to.
    /// </summary>
    public class InMemoryRetryTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public InMemoryRetryTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record InMemoryRetryEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task HandlerSucceedsOnALaterAttempt_IsAckedWithoutEverDeclaringARetryQueue()
        {
            const string queueName = "inmemory.succeeds.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.MaxDeliveryRetryAttempts = 3;
                r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(100);
                r.MaxDeliveryRetryDelay = TimeSpan.FromMilliseconds(500);
            });

            await using var _ = services;
            await using var __ = bus;

            var attempts = 0;
            await bus.SubscribeAsync<InMemoryRetryEvent>(queueName, _ =>
            {
                // Fails twice, then succeeds on the third (and last allowed) attempt.
                if (Interlocked.Increment(ref attempts) < 3)
                {
                    throw new InvalidOperationException("not yet");
                }

                return Task.CompletedTask;
            });

            await bus.PublishAsync(new InMemoryRetryEvent(Guid.NewGuid()));

            // Not queue depth: the broker delivers the message to the consumer (and drops it from the
            // "ready" count) the moment it is published, well before the in-memory retries finish -
            // prefetch holds it unacked, but "ready" is already 0 regardless of how the handler goes.
            await Wait.UntilAsync(
                () => Volatile.Read(ref attempts) == 3,
                "the handler succeeds on its third and last allowed attempt");

            // If the ack had failed, the broker would eventually redeliver the message and the
            // handler would run a fourth time.
            await Wait.StaysFalseAsync(
                () => Volatile.Read(ref attempts) > 3,
                "the already-acked message being redelivered");

            Assert.Equal(0u, await _fixture.GetQueueDepthAsync(queueName));
            Assert.Equal(0u, await _fixture.GetQueueDepthAsync($"{queueName}.deadletter"));

            // The default strategy never declares a retry queue at all.
            Assert.False(await _fixture.QueueExistsAsync($"{queueName}.retry"));
        }

        [Fact]
        public async Task HandlerAlwaysFails_ReachesTheDeadLetterQueueWithTheRealAttemptCount()
        {
            const string queueName = "inmemory.exhausted.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.MaxDeliveryRetryAttempts = 2;
                r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(100);
                r.MaxDeliveryRetryDelay = TimeSpan.FromMilliseconds(200);
            });

            await using var _ = services;
            await using var __ = bus;

            var attempts = 0;
            await bus.SubscribeAsync<InMemoryRetryEvent>(queueName, _ =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("always fails");
            });

            await bus.PublishAsync(new InMemoryRetryEvent(Guid.NewGuid()));

            await Wait.UntilAsync(
                async () => await _fixture.GetQueueDepthAsync($"{queueName}.deadletter") == 1,
                "the exhausted message reaches its dead-letter queue");

            // The initial delivery plus both configured retries - there is no x-retry-count header
            // to read this back from (nothing was ever republished), so Bus passes the real count.
            Assert.Equal(3, Volatile.Read(ref attempts));

            await using var rawConnection = await _fixture.CreateRawConnectionAsync();
            await using var rawChannel = await rawConnection.CreateChannelAsync();

            var result = await rawChannel.BasicGetAsync($"{queueName}.deadletter", autoAck: true);

            Assert.NotNull(result);
            Assert.Equal(2, Convert.ToInt32(result!.BasicProperties.Headers!["x-final-retry-count"]));
        }

        [Fact]
        public async Task CancellingTheSubscription_DuringTheBackoffDelay_RequeuesInsteadOfLosingTheMessage()
        {
            const string queueName = "inmemory.cancelled.queue";

            var (services, bus) = BusTestHost.Create(_fixture, configureResilience: r =>
            {
                r.MaxDeliveryRetryAttempts = 5;
                r.InitialDeliveryRetryDelay = TimeSpan.FromSeconds(5);
                r.MaxDeliveryRetryDelay = TimeSpan.FromSeconds(5);
            });

            await using var _ = services;
            await using var __ = bus;

            using var subscriptionLifetime = new CancellationTokenSource();
            var attempts = 0;
            var firstAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            await bus.SubscribeAsync<InMemoryRetryEvent>(
                queueName,
                _ =>
                {
                    Interlocked.Increment(ref attempts);
                    firstAttempt.TrySetResult();
                    throw new InvalidOperationException("always fails");
                },
                subscriptionLifetime.Token);

            await bus.PublishAsync(new InMemoryRetryEvent(Guid.NewGuid()));

            // Cancel while the first failure is waiting out its (5s) backoff delay.
            await firstAttempt.Task.WaitAsync(TimeSpan.FromSeconds(15));
            subscriptionLifetime.Cancel();

            await Wait.UntilAsync(
                async () => await _fixture.GetConsumerCountAsync(queueName) == 0,
                "the cancelled subscription has stopped consuming");

            await Wait.UntilAsync(
                async () => await _fixture.GetQueueDepthAsync(queueName) == 1,
                "the delivery was requeued instead of lost");

            // Only the one attempt the cancellation interrupted - it was never retried in-process,
            // because there was no x-retry-count to carry the attempt count past the requeue.
            Assert.Equal(1, Volatile.Read(ref attempts));
            Assert.Equal(0u, await _fixture.GetQueueDepthAsync($"{queueName}.deadletter"));
        }
    }
}
