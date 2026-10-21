using RabbitMQ.Client;
using RabbitSharp.Abstractions;
using System.Collections.Concurrent;
using System.Text.Json;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// RabbitMQ.Client's automatic recovery puts a dropped subscription back by itself, re-registering
    /// the same consumer on a recovered channel. The bus has to survive that without losing track of
    /// the subscription: treating the consumer-cancel and channel-shutdown that a connection loss
    /// raises as a real interruption drops the registration, and then the subscription the client just
    /// restored is one the bus can neither stop nor report on again.
    /// </summary>
    public class RecoveryBookkeepingTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public RecoveryBookkeepingTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record BookkeepingEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task TransientConnectionDrop_ReportsNoInterruptionBecauseRecoveryUndoesIt()
        {
            const string clientName = "bookkeeping-no-false-alarm";
            const string queueName = "bookkeeping.noalarm.queue";

            var (services, bus) = BusTestHost.Create(_fixture, clientProvidedName: clientName);
            await using var _ = services;
            await using var __ = bus;

            var interruptions = new ConcurrentBag<SubscriptionInterruptedEventArgs>();
            bus.SubscriptionInterrupted += args =>
            {
                interruptions.Add(args);
                return Task.CompletedTask;
            };

            var received = new ConcurrentBag<Guid>();
            await bus.SubscribeAsync<BookkeepingEvent>(queueName,
                msg => { received.Add(msg.CorrelationId); return Task.CompletedTask; });

            var baseline = new BookkeepingEvent(Guid.NewGuid());
            await bus.PublishAsync(baseline);
            await Wait.UntilAsync(() => received.Contains(baseline.CorrelationId), "the baseline message is handled");

            await DropConnectionAsync(clientName);

            // Recovery has finished once the original subscription - never re-created by this test -
            // handles a message again.
            var afterRecovery = new BookkeepingEvent(Guid.NewGuid());
            await Wait.UntilAsync(
                async () => await Record.ExceptionAsync(() => bus.PublishAsync(afterRecovery)) is null,
                "a publish succeeds again after the connection was dropped",
                TimeSpan.FromSeconds(60));

            await Wait.UntilAsync(
                () => received.Contains(afterRecovery.CorrelationId),
                "the recovered subscription handles a new message");

            // The regression: a connection blip that automatic recovery fully undid was reported as
            // "this subscription has stopped consuming and will not recover on its own". An
            // application that re-subscribes on this event adds a second consumer to the queue per
            // blip, because the first one is already back.
            Assert.Empty(interruptions);
        }

        [Fact]
        public async Task AfterAutomaticRecovery_DisposingTheBusStillStopsTheSubscription()
        {
            const string clientName = "bookkeeping-dispose-after-recovery";
            const string queueName = "bookkeeping.dispose.queue";

            var (services, bus) = BusTestHost.Create(_fixture, clientProvidedName: clientName);
            await using var _ = services;

            var received = new ConcurrentBag<Guid>();
            await bus.SubscribeAsync<BookkeepingEvent>(queueName,
                msg => { received.Add(msg.CorrelationId); return Task.CompletedTask; });

            var baseline = new BookkeepingEvent(Guid.NewGuid());
            await bus.PublishAsync(baseline);
            await Wait.UntilAsync(() => received.Contains(baseline.CorrelationId), "the baseline message is handled");

            await DropConnectionAsync(clientName);

            var afterRecovery = new BookkeepingEvent(Guid.NewGuid());
            await Wait.UntilAsync(
                async () => await Record.ExceptionAsync(() => bus.PublishAsync(afterRecovery)) is null,
                "a publish succeeds again after the connection was dropped",
                TimeSpan.FromSeconds(60));

            await Wait.UntilAsync(
                () => received.Contains(afterRecovery.CorrelationId),
                "the recovered subscription handles a new message");

            // Only the bus is disposed - the connection manager stays alive, so the connection is
            // still up and the only thing that can stop this consumer is the bus knowing about it.
            await bus.DisposeAsync();

            // Published without the bus, so the publish itself cannot fail on a disposed bus.
            var afterDispose = new BookkeepingEvent(Guid.NewGuid());
            await PublishRawAsync(afterDispose);

            // The regression: recovery had wiped the registration, so DisposeAsync cancelled nothing
            // and the handler kept running - against an application whose DI scopes are already gone.
            await Wait.StaysFalseAsync(
                () => received.Contains(afterDispose.CorrelationId),
                "the handler running after the bus was disposed",
                TimeSpan.FromSeconds(5));
        }

        private async Task DropConnectionAsync(string clientProvidedName)
        {
            var connectionName = await _fixture.FindConnectionNameAsync(clientProvidedName);
            Assert.NotNull(connectionName);

            await _fixture.ForceCloseConnectionAsync(connectionName!);
        }

        private async Task PublishRawAsync(BookkeepingEvent message)
        {
            await using var connection = await _fixture.CreateRawConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            await channel.BasicPublishAsync(
                "bookkeepingevent",
                "BookkeepingEvent",
                mandatory: true,
                new BasicProperties { MessageId = message.CorrelationId.ToString(), Persistent = true },
                JsonSerializer.SerializeToUtf8Bytes(message));
        }
    }
}
