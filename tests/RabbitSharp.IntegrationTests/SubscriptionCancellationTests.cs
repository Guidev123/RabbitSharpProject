using RabbitSharp.Abstractions;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// <c>SubscribeAsync</c> registers a callback on the subscription's cancellation token so the
    /// subscription stops when the token fires. The token is usually the application's lifetime
    /// token, which outlives every subscription made with it - so the registration has to be released
    /// when the subscription stops, or the token holds that callback, and through it the bus and its
    /// channel, for the rest of the process.
    /// </summary>
    public class SubscriptionCancellationTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public SubscriptionCancellationTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record CancellationEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task CancellingTheSubscriptionToken_StopsConsumingAndLeavesTheMessagesQueued()
        {
            const string queueName = "cancellation.stops.queue";

            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            using var subscriptionLifetime = new CancellationTokenSource();
            var received = new ConcurrentBag<Guid>();

            await bus.SubscribeAsync<CancellationEvent>(queueName,
                msg => { received.Add(msg.CorrelationId); return Task.CompletedTask; },
                subscriptionLifetime.Token);

            var beforeCancellation = new CancellationEvent(Guid.NewGuid());
            await bus.PublishAsync(beforeCancellation);
            await Wait.UntilAsync(
                () => received.Contains(beforeCancellation.CorrelationId),
                "the subscription is consuming");

            subscriptionLifetime.Cancel();

            // The stop runs off the cancellation callback, so wait for the broker to agree the
            // consumer is gone rather than racing it.
            await Wait.UntilAsync(
                async () => await _fixture.GetConsumerCountAsync(queueName) == 0,
                "the broker has no consumer left on the queue");

            var afterCancellation = new CancellationEvent(Guid.NewGuid());
            await bus.PublishAsync(afterCancellation);

            await Wait.StaysFalseAsync(
                () => received.Contains(afterCancellation.CorrelationId),
                "the cancelled subscription handling a new message");

            // Nothing was consumed and nothing was lost - it is simply waiting for a new subscriber.
            Assert.Equal(1u, await _fixture.GetQueueDepthAsync(queueName));
        }

        [Fact]
        public async Task StoppedSubscription_IsNotKeptAliveByALongLivedCancellationToken()
        {
            // Stands in for IHostApplicationLifetime.ApplicationStopping: one token, reused for
            // every subscription, alive for as long as the process is.
            using var applicationStopping = new CancellationTokenSource();

            var busReference = await SubscribeThenDisposeAsync(applicationStopping.Token);

            await CollectAsync();

            Assert.False(
                busReference.IsAlive,
                "The bus is still reachable after being disposed. SubscribeAsync's cancellation " +
                "registration was never released, so the long-lived token still holds its callback - " +
                "and through the callback's state, the bus and its channel - for the life of the " +
                "process. One leaked bus and channel per subscription.");
        }

        /// <summary>
        /// The harder case, and the one that compounds: a subscription the broker interrupts is
        /// deregistered on a different path from <c>DisposeAsync</c>, while the application keeps
        /// running and its lifetime token stays alive. An application that re-subscribes on
        /// <see cref="IBus.SubscriptionInterrupted"/> - which is what that event is for - would leak
        /// one bus and one channel per interruption.
        /// </summary>
        [Fact]
        public async Task SubscriptionInterruptedByTheBroker_IsNotKeptAliveByALongLivedCancellationToken()
        {
            using var applicationStopping = new CancellationTokenSource();

            var busReference = await SubscribeThenLoseTheQueueAsync(applicationStopping.Token);

            await CollectAsync();

            Assert.False(
                busReference.IsAlive,
                "The bus is still reachable after its subscription was interrupted and the bus " +
                "disposed. The interruption deregistered the subscription without releasing its " +
                "cancellation registration, so the long-lived token keeps holding the bus.");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private async Task<WeakReference> SubscribeThenLoseTheQueueAsync(CancellationToken applicationStopping)
        {
            const string queueName = "cancellation.interrupted.queue";

            var (services, bus) = BusTestHost.Create(_fixture);

            var interrupted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            bus.SubscriptionInterrupted += _ =>
            {
                interrupted.TrySetResult();
                return Task.CompletedTask;
            };

            await bus.SubscribeAsync<CancellationEvent>(
                queueName,
                static _ => Task.CompletedTask,
                applicationStopping);

            // An operator recreating the queue, a policy change, a failover: the broker cancels the
            // consumer while the connection stays healthy.
            await using (var connection = await _fixture.CreateRawConnectionAsync())
            await using (var channel = await connection.CreateChannelAsync())
            {
                await channel.QueueDeleteAsync(queueName, ifUnused: false, ifEmpty: false);
            }

            await interrupted.Task.WaitAsync(TimeSpan.FromSeconds(15));

            await bus.DisposeAsync();
            await services.DisposeAsync();

            return new WeakReference(bus);
        }

        /// <summary>
        /// Everything that could reach the bus is local to this method, so once it has returned the
        /// only thing that can still be holding the bus is the cancellation token registration.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private async Task<WeakReference> SubscribeThenDisposeAsync(CancellationToken applicationStopping)
        {
            var (services, bus) = BusTestHost.Create(_fixture);

            await bus.SubscribeAsync<CancellationEvent>(
                "cancellation.leak.queue",
                static _ => Task.CompletedTask,
                applicationStopping);

            await bus.DisposeAsync();
            await services.DisposeAsync();

            return new WeakReference(bus);
        }

        private static async Task CollectAsync()
        {
            // More than one pass: the subscription's async state machines have to go first, and a
            // channel's finalizer can be what drops the last reference to it.
            for (var attempt = 0; attempt < 5; attempt++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                await Task.Yield();
            }
        }
    }
}
