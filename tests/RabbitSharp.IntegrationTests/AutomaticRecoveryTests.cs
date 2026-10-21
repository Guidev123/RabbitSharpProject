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

            await Task.Delay(500);

            var baseline = new RecoveryTestEvent(Guid.NewGuid());
            await bus.PublishAsync(baseline);
            await Task.Delay(800);
            Assert.Contains(baseline.CorrelationId, received);

            var connectionName = await _fixture.FindConnectionNameAsync(clientName);
            Assert.NotNull(connectionName);

            await _fixture.ForceCloseConnectionAsync(connectionName!);

            // Give RabbitMQ.Client's own automatic recovery time to reconnect on its own.
            await Task.Delay(6000);

            var afterRecovery = new RecoveryTestEvent(Guid.NewGuid());
            await bus.PublishAsync(afterRecovery);
            await Task.Delay(1500);

            // The subscription was never re-created by this test - if it still receives messages,
            // the original consumer channel survived the forced disconnect via RabbitMQ.Client's
            // own automatic + topology recovery, exactly as intended.
            Assert.Contains(afterRecovery.CorrelationId, received);
        }
    }
}
