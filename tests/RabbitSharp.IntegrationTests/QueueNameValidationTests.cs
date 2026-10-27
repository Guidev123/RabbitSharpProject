using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// An empty queue name is not an empty name to AMQP - it means "the last queue declared on this
    /// channel". <c>DeclareInfrastructureAsync</c> declares the main queue, the dead-letter queue and
    /// then the retry queue (under <c>RetryStrategy.RetryQueue</c>), so the last declared queue would
    /// be a retry queue: the bind and the consume that follow would silently attach to that instead.
    /// The subscription would come up, consume from the retry queue, and the main queue it was
    /// supposed to serve would be a server-named queue nothing is bound to - which is why
    /// <c>Validate</c> rejects a blank name before any of this is declared.
    /// </summary>
    public class QueueNameValidationTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public QueueNameValidationTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record ValidationEvent(Guid CorrelationId) : IMessage;

        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        public async Task SubscribeAsyncWithABlankQueueName_IsRejectedInsteadOfBindingTheWrongQueue(string queueName)
        {
            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            await Assert.ThrowsAsync<ArgumentException>(
                () => bus.SubscribeAsync<ValidationEvent>(queueName, _ => Task.CompletedTask));

            // The regression: the derived names were built off the blank one, so the broker ended up
            // with a ".deadletter" and a ".retry" queue, and the consumer was attached to the latter.
            Assert.False(await _fixture.QueueExistsAsync(".deadletter"), "a dead-letter queue was declared for a blank queue name.");
            Assert.False(await _fixture.QueueExistsAsync(".retry"), "a retry queue was declared for a blank queue name.");
        }

        [Fact]
        public async Task SubscribeAsyncWithOptionsCarryingABlankMainQueueName_IsRejected()
        {
            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            // Built by hand rather than through the builder, which is the other way these options
            // reach SubscribeAsync.
            var options = new Options.BusInfrastructureOptions
            {
                RoutingKey = "ValidationEvent",
                BindingPattern = "ValidationEvent.#"
            };
            options.Exchange.ExchangeName = "validationevent";
            options.MainQueue.QueueName = string.Empty;
            options.RetryQueue.QueueName = "optionsblank.queue.retry";
            options.DeadLetterQueue.QueueName = "optionsblank.queue.deadletter";

            await Assert.ThrowsAsync<ArgumentException>(
                () => bus.SubscribeAsync<ValidationEvent>(options, _ => Task.CompletedTask));
        }

        [Fact]
        public void BusInfrastructureBuilderWithABlankQueueName_IsRejectedAtConstruction()
        {
            // The earliest place this can be caught, and the one with the clearest stack trace.
            Assert.Throws<ArgumentException>(() => new BusInfrastructureBuilder(string.Empty));
        }
    }
}
