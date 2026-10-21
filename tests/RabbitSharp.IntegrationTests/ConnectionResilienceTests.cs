using RabbitMQ.Client.Exceptions;
using RabbitSharp.Abstractions;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    public class ConnectionResilienceTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public ConnectionResilienceTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record ZeroRetryEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task TryConnectMaxRetriesZero_ConnectsWithoutThrowingAtStartup()
        {
            // TryConnectMaxRetries = 0 means "try once, no retries" - ReconnectAsync's retry loop
            // must treat that as "never satisfies attempt < TryConnectMaxRetries" rather than as a
            // misconfiguration.
            var (services, bus) = BusTestHost.Create(_fixture, configureBroker: b => b.TryConnectMaxRetries = 0);
            await using var _ = services;
            await using var __ = bus;

            // The exchange has nothing bound to it, so PublishException (unroutable) is expected
            // and fine here - what matters is that the connection itself succeeded.
            var exception = await Record.ExceptionAsync(() => bus.PublishAsync(new ZeroRetryEvent(Guid.NewGuid())));

            Assert.True(exception is null or PublishException);
        }
    }
}
