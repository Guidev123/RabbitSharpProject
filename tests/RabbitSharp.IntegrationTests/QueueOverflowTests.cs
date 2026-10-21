using RabbitMQ.Client.Exceptions;
using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    public class QueueOverflowTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public QueueOverflowTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record OverflowEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task PublishingToAFullQueue_ThrowsInsteadOfDestroyingTheOldestMessage()
        {
            const string queueName = "overflow.reject.queue";
            const int maxLength = 3;

            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var handlerGate = new TaskCompletionSource();

            // The handler blocks, so messages pile up in the queue and the length limit is reached.
            await bus.SubscribeAsync<OverflowEvent>(
                new BusInfrastructureBuilder(queueName)
                    .WithMainQueue(q => q.WithMaxLength(maxLength))
                    .Build(),
                _ => handlerGate.Task);

            var published = 0;
            Exception? overflowException = null;

            for (var i = 0; i < 40 && overflowException is null; i++)
            {
                overflowException = await Record.ExceptionAsync(() => bus.PublishAsync(new OverflowEvent(Guid.NewGuid())));

                if (overflowException is null)
                {
                    published++;
                }
            }

            handlerGate.SetResult();

            // The publisher is told, rather than finding out never: the broker nacks the publish once
            // the queue is full, which surfaces here as a PublishException.
            Assert.NotNull(overflowException);
            Assert.IsType<PublishException>(overflowException);
            Assert.InRange(published, 1, 39);
        }
    }
}
