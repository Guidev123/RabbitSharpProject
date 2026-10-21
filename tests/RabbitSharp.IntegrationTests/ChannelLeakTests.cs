using Microsoft.Extensions.DependencyInjection;
using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// A <c>SubscribeAsync</c> that fails after opening its channel must close it. The channel is
    /// opened before anything else happens and is registered only once <c>BasicConsumeAsync</c> has
    /// returned, so a failure in between leaves an open channel nothing has a reference to - and the
    /// connection runs out of channels (2047 by default) one failed attempt at a time.
    /// </summary>
    public class ChannelLeakTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public ChannelLeakTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record LeakProbeEvent(Guid CorrelationId) : IMessage;
        private sealed record PoisonSubscribeEvent(Guid CorrelationId) : IMessage;

        /// <summary>
        /// Fails for one message type only, and client-side: the broker never learns about it, so it
        /// has no reason to close the channel on its own. That is what makes this a real leak test -
        /// the 406 path the suite already covers does not leak, because there the broker closes the
        /// channel for us.
        /// </summary>
        private sealed class ThrowingNamingConventions : SimpleNamingConventions
        {
            public override string ExchangeNamingConvention(Type messageType)
            {
                return messageType == typeof(PoisonSubscribeEvent)
                    ? throw new InvalidOperationException("naming convention failed for this message type")
                    : base.ExchangeNamingConvention(messageType);
            }
        }

        [Fact]
        public async Task SubscribeAsyncFailingAfterTheChannelIsOpen_ClosesItInsteadOfLeakingIt()
        {
            const int failedAttempts = 10;

            var (services, bus) = BusTestHost.Create(
                _fixture,
                configureServices: s => s.AddSingleton<INamingConventions, ThrowingNamingConventions>());

            await using var _ = services;
            await using var __ = bus;

            // One healthy subscription, so the baseline is the publish channel plus this
            // subscription's own channel.
            await bus.SubscribeAsync<LeakProbeEvent>("leak.probe.queue", _ => Task.CompletedTask);

            var baseline = 0;
            await Wait.UntilAsync(
                async () => (baseline = await _fixture.CountBrokerChannelsAsync()) > 0,
                "the healthy subscription's channels are open");

            for (var attempt = 0; attempt < failedAttempts; attempt++)
            {
                // Exchange name left unset on purpose: that is what makes the naming convention run
                // inside DeclareInfrastructureAsync, after the consumer channel has been opened.
                var options = new BusInfrastructureBuilder($"leak.poison.{attempt}.queue")
                    .WithRoutingKey("PoisonSubscribeEvent")
                    .Build();

                await Assert.ThrowsAsync<InvalidOperationException>(
                    () => bus.SubscribeAsync<PoisonSubscribeEvent>(options, _ => Task.CompletedTask));
            }

            // Asserted with no grace period on purpose. The close is awaited inside SubscribeAsync, so
            // the broker has already processed it by the time the call returns; waiting here would
            // instead give the GC time to finalize leaked channels and close them anyway, which is
            // exactly how this test would stop detecting the leak.
            var afterFailedAttempts = await _fixture.CountBrokerChannelsAsync();

            Assert.Equal(baseline, afterFailedAttempts);
        }
    }
}
