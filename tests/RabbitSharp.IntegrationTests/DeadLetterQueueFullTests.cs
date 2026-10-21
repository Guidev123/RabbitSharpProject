using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitSharp.Abstractions;
using RabbitSharp.Builders;
using RabbitSharp.Enums;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// The dead-letter queue is the end of the line: when it will not take a message there is nowhere
    /// left to put it, and the one thing that must not happen is losing it. Nacking without requeue
    /// here dead-letters the message through the main queue's dead-letter exchange - into the very
    /// dead-letter queue that just rejected it - and RabbitMQ silently discards a dead-lettered
    /// message a <c>reject-publish</c> queue refuses. The message is destroyed with nothing anywhere
    /// to replay and nothing but a log line to say so.
    /// </summary>
    public class DeadLetterQueueFullTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public DeadLetterQueueFullTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record DlqFullEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task DeadLetterQueueAtItsLengthLimit_HoldsTheMessageBackInsteadOfDestroyingIt()
        {
            const string queueName = "dlqfull.queue";
            const int published = 3;

            var logs = new TestLoggerProvider();

            var (services, bus) = BusTestHost.Create(
                _fixture,
                // No retries, so every failure goes straight at the dead-letter queue.
                configureResilience: r => r.MaxDeliveryRetryAttempts = 0,
                configureServices: s => s.AddLogging(b => b.AddProvider(logs)));

            await using var _ = services;
            await using var __ = bus;

            var interruptions = new List<SubscriptionInterruptedEventArgs>();
            bus.SubscriptionInterrupted += args =>
            {
                lock (interruptions) interruptions.Add(args);
                return Task.CompletedTask;
            };

            await bus.SubscribeAsync<DlqFullEvent>(
                new BusInfrastructureBuilder(queueName)
                    .WithExchange(e =>
                    {
                        e.WithName("dlqfullevent");
                        e.WithType(ExchangeTypeEnum.Topic);
                    })
                    .WithRoutingKey("DlqFullEvent")
                    .WithBindingPattern("DlqFullEvent.#")
                    // Room for exactly one dead letter: the second failure has nowhere to go.
                    .WithDeadLetterQueue(d => d.WithMaxLength(1))
                    .Build(),
                _ => throw new InvalidOperationException("always fails"));

            for (var i = 0; i < published; i++)
            {
                await bus.PublishAsync(new DlqFullEvent(Guid.NewGuid()));
            }

            await Wait.UntilAsync(
                () => logs.Records.Any(r => r.EventName == "LogFailureHandlingError"),
                "the dead-letter publish has been rejected");

            // The subscription stops rather than spinning on a message it cannot move, and says so.
            await Wait.UntilAsync(
                () =>
                {
                    lock (interruptions) return interruptions.Count > 0;
                },
                "the subscription reports that it can no longer handle failures");

            SubscriptionInterruptedEventArgs interruption;
            lock (interruptions) interruption = interruptions[0];

            Assert.Equal(queueName, interruption.QueueName);
            Assert.Equal(SubscriptionInterruptionReason.FailureHandlingUnavailable, interruption.Reason);

            // Everything unacked comes back to the main queue once the subscription's channel closes,
            // so wait for the broker to settle before counting.
            await Wait.UntilAsync(
                async () => await _fixture.GetQueueDepthAsync(queueName) + await _fixture.GetQueueDepthAsync($"{queueName}.deadletter") == published,
                "every published message is accounted for in the main or the dead-letter queue");

            var main = await _fixture.GetQueueDepthAsync(queueName);
            var deadLetter = await _fixture.GetQueueDepthAsync($"{queueName}.deadletter");

            // The regression: the dead-letter queue took one, and the rest were silently destroyed -
            // nacked without requeue, dead-lettered into the full queue, and dropped by the broker.
            Assert.Equal(1u, deadLetter);
            Assert.Equal((uint)published - 1, main);
        }
    }
}
