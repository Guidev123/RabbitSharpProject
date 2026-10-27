using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitSharp.Abstractions;
using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// A handler that succeeded and an ack that then failed is an infrastructure failure, not a
    /// handler failure. Retrying or dead-lettering it would duplicate the business effect of work
    /// that already ran, and reporting it as "Error processing message" would send whoever reads the
    /// log after the handler instead of after the connection.
    /// </summary>
    public class AckFailureTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public AckFailureTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record AckFailureEvent(Guid CorrelationId) : IMessage;

        [Fact]
        public async Task HandlerSucceedsButTheAckFails_IsReportedAsSuchAndNotRetriedOrDeadLettered()
        {
            const string clientName = "ack-failure-test";
            const string queueName = "ackfailure.queue";

            var logs = new TestLoggerProvider();

            var (services, bus) = BusTestHost.Create(
                _fixture,
                clientProvidedName: clientName,
                // Automatic recovery must not reconnect while the ack is being attempted, otherwise
                // what the ack hits depends on timing rather than on the scenario.
                configureBroker: b => b.NetworkRecoveryIntervalInSeconds = 60,
                configureResilience: r =>
                {
                    r.Strategy = RetryStrategy.RetryQueue;
                    r.MaxDeliveryRetryAttempts = 3;
                    r.InitialDeliveryRetryDelay = TimeSpan.FromMilliseconds(200);
                },
                configureServices: s => s.AddLogging(b => b.AddProvider(logs)));

            await using var _ = services;
            await using var __ = bus;

            var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var connectionKilled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var handlerCompletions = 0;

            await bus.SubscribeAsync<AckFailureEvent>(queueName, async _ =>
            {
                handlerEntered.TrySetResult();

                // The handler finishes only once the connection is gone, so the ack that follows it
                // is the thing that fails.
                await connectionKilled.Task;

                Interlocked.Increment(ref handlerCompletions);
            });

            await bus.PublishAsync(new AckFailureEvent(Guid.NewGuid()));

            await handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(30));

            var connectionName = await _fixture.FindConnectionNameAsync(clientName);
            Assert.NotNull(connectionName);

            await _fixture.ForceCloseConnectionAsync(connectionName!);

            await Wait.UntilAsync(
                async () => await _fixture.FindConnectionNameAsync(clientName, maxAttempts: 1) is null,
                "the broker has dropped the connection",
                TimeSpan.FromSeconds(30));

            connectionKilled.SetResult();

            await Wait.UntilAsync(
                () => logs.Records.Any(r => r.EventName == "LogAckFailed"),
                "the failed ack is reported as a failed ack");

            // The regression: this used to surface as LogProcessingError - the handler blamed for a
            // failure it had nothing to do with - and the message pushed onto the retry path.
            Assert.DoesNotContain(logs.Records, r => r.EventName == "LogProcessingError");
            Assert.DoesNotContain(logs.Records, r => r.EventName == "LogSentToRetryQueue");
            Assert.DoesNotContain(logs.Records, r => r.EventName == "LogSentToDeadLetterQueue");

            Assert.Equal(1, Volatile.Read(ref handlerCompletions));

            // The broker requeues what was never acked, so the message is still there to be
            // redelivered - it was not copied onto the retry path, and nothing was lost.
            Assert.Equal(0u, await _fixture.GetQueueDepthAsync($"{queueName}.retry"));
            Assert.Equal(0u, await _fixture.GetQueueDepthAsync($"{queueName}.deadletter"));
            Assert.Equal(1u, await _fixture.GetQueueDepthAsync(queueName));

            var ackFailure = logs.Records.First(r => r.EventName == "LogAckFailed");
            Assert.Equal(LogLevel.Error, ackFailure.Level);
            Assert.NotNull(ackFailure.Exception);
        }
    }
}
