using RabbitSharp.Abstractions;
using System.Collections.Concurrent;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    public class AutomaticRecoveryTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public AutomaticRecoveryTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record RecoveryTestEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task ForcedConnectionDrop_OriginalSubscriptionSurvivesViaAutomaticRecovery()
        {
            const string clientName = "recovery-test-connection";

            var (services, bus) = BusTestHost.Create(_fixture, clientProvidedName: clientName);
            await using var _ = services;
            await using var __ = bus;

            var received = new ConcurrentBag<Guid>();
            await bus.SubscribeAsync<RecoveryTestEvent>("recovery.queue",
                msg => { received.Add(msg.CorrelationId); return Task.CompletedTask; });

            var baseline = new RecoveryTestEvent(Guid.NewGuid());
            await bus.PublishAsync(baseline);

            await Wait.UntilAsync(() => received.Contains(baseline.CorrelationId), "the baseline message is handled");

            var connectionName = await _fixture.FindConnectionNameAsync(clientName);
            Assert.NotNull(connectionName);

            await _fixture.ForceCloseConnectionAsync(connectionName!);

            // The publish itself waits for RabbitMQ.Client's automatic recovery to finish, so there is
            // nothing to sleep for - but the first attempt can still land on the dying connection.
            var afterRecovery = new RecoveryTestEvent(Guid.NewGuid());
            await Wait.UntilAsync(
                async () => await Record.ExceptionAsync(() => bus.PublishAsync(afterRecovery)) is null,
                "a publish succeeds again after the connection was dropped",
                TimeSpan.FromSeconds(60));

            // The subscription was never re-created by this test - if it still receives messages,
            // the original consumer channel survived the forced disconnect via RabbitMQ.Client's
            // own automatic + topology recovery, exactly as intended.
            await Wait.UntilAsync(
                () => received.Contains(afterRecovery.CorrelationId),
                "the recovered subscription handles a new message");
        }
    }
}
