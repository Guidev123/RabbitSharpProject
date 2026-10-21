using RabbitSharp.Abstractions;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    public class SubscriptionInterruptionTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public SubscriptionInterruptionTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record InterruptionEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task QueueDeletedOutOfBand_RaisesSubscriptionInterruptedInsteadOfDyingInSilence()
        {
            const string queueName = "interruption.deleted.queue";

            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var interrupted = new TaskCompletionSource<SubscriptionInterruptedEventArgs>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            bus.SubscriptionInterrupted += args =>
            {
                interrupted.TrySetResult(args);
                return Task.CompletedTask;
            };

            await bus.SubscribeAsync<InterruptionEvent>(queueName, _ => Task.CompletedTask);

            // What an operator recreating a queue, a policy change or a cluster failover looks like to
            // the consumer: the broker cancels it, and nothing throws anywhere.
            await using (var connection = await _fixture.CreateRawConnectionAsync())
            await using (var channel = await connection.CreateChannelAsync())
            {
                await channel.QueueDeleteAsync(queueName, ifUnused: false, ifEmpty: false);
            }

            var args = await interrupted.Task.WaitAsync(TimeSpan.FromSeconds(15));

            Assert.Equal(queueName, args.QueueName);
        }
    }
}
