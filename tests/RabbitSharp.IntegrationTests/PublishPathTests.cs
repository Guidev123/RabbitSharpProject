using Microsoft.Extensions.DependencyInjection;
using RabbitSharp.Abstractions;
using RabbitSharp.Enums;
using System.Collections.Concurrent;
using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// The convention-based <c>PublishAsync</c> overload is the hot path, and it used to rebuild the
    /// whole publisher configuration for every single message - a builder, an options graph and a
    /// freshly lowercased exchange name per publish - and to track which exchanges it had declared
    /// against a plain field holding the last channel, written and read with no synchronisation while
    /// publishes run concurrently.
    /// </summary>
    public class PublishPathTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public PublishPathTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        private sealed record FirstPathEvent(Guid CorrelationId) : IMessage;
        private sealed record SecondPathEvent(Guid CorrelationId) : IMessage;
        private sealed record CacheProbeEvent(Guid CorrelationId) : IMessage;

        /// <summary>
        /// Counts what the bus asks of the naming conventions. They are pure functions of the message
        /// type, so asking them once per type rather than once per message is the observable shape of
        /// the caching - and the thing that was allocating on every publish.
        /// </summary>
        private sealed class CountingNamingConventions : SimpleNamingConventions
        {
            private readonly ConcurrentDictionary<Type, int> _exchangeNameCalls = new();

            public int ExchangeNameCallsFor(Type messageType) => _exchangeNameCalls.GetValueOrDefault(messageType);

            public override string ExchangeNamingConvention(Type messageType)
            {
                _exchangeNameCalls.AddOrUpdate(messageType, 1, static (_, count) => count + 1);
                return base.ExchangeNamingConvention(messageType);
            }
        }

        [Fact]
        public async Task ConventionPublish_ResolvesTheConventionsOncePerMessageTypeRatherThanPerMessage()
        {
            const int publishesPerType = 25;

            var conventions = new CountingNamingConventions();

            var (services, bus) = BusTestHost.Create(
                _fixture,
                configureServices: s => s.AddSingleton<INamingConventions>(conventions));

            await using var _ = services;
            await using var __ = bus;

            // Subscribed so the publishes are routable - a mandatory publish with nothing bound fails.
            await bus.SubscribeAsync<FirstPathEvent>("publishpath.first.queue", static _ => Task.CompletedTask);
            await bus.SubscribeAsync<SecondPathEvent>("publishpath.second.queue", static _ => Task.CompletedTask);

            var subscribeCallsFirst = conventions.ExchangeNameCallsFor(typeof(FirstPathEvent));
            var subscribeCallsSecond = conventions.ExchangeNameCallsFor(typeof(SecondPathEvent));

            for (var i = 0; i < publishesPerType; i++)
            {
                await bus.PublishAsync(new FirstPathEvent(Guid.NewGuid()));
                await bus.PublishAsync(new SecondPathEvent(Guid.NewGuid()));
            }

            // The regression: one call per publish, each allocating a new lowercased exchange name on
            // top of a builder and an options graph.
            Assert.Equal(subscribeCallsFirst + 1, conventions.ExchangeNameCallsFor(typeof(FirstPathEvent)));
            Assert.Equal(subscribeCallsSecond + 1, conventions.ExchangeNameCallsFor(typeof(SecondPathEvent)));
        }

        /// <summary>
        /// A guard, not a regression test: the exchange-declared cache must not outlive the channel it
        /// was filled for. The pre-existing code did invalidate it, so this passes either way - it is
        /// here so that moving the cache onto the channel cannot quietly drop the invalidation.
        /// </summary>
        [Fact]
        public async Task ExchangeDeclaredCache_DoesNotOutliveTheChannelItWasFilledFor()
        {
            const string exchangeName = "cacheprobeevent";

            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            await bus.SubscribeAsync<CacheProbeEvent>("publishpath.cache.queue", static _ => Task.CompletedTask);

            // Declares the exchange and records it as declared for this channel.
            await bus.PublishAsync(new CacheProbeEvent(Guid.NewGuid()));
            Assert.True(await _fixture.ExchangeExistsAsync(exchangeName));

            await using (var connection = await _fixture.CreateRawConnectionAsync())
            await using (var channel = await connection.CreateChannelAsync())
            {
                await channel.ExchangeDeleteAsync(exchangeName);
            }

            await Wait.UntilAsync(
                async () => !await _fixture.ExchangeExistsAsync(exchangeName),
                "the exchange is gone from the broker");

            // The cache says the exchange is declared, so this skips the declare and publishes into
            // an exchange that no longer exists: the broker answers 404 and closes the publish
            // channel. That is what makes the next publish get a brand-new channel.
            Assert.NotNull(await Record.ExceptionAsync(() => bus.PublishAsync(new CacheProbeEvent(Guid.NewGuid()))));

            // New channel, so nothing may be assumed declared on it. This publish is unroutable - the
            // binding went with the deleted exchange - but it has to redeclare the exchange first.
            await Wait.UntilAsync(
                async () =>
                {
                    await Record.ExceptionAsync(() => bus.PublishAsync(new CacheProbeEvent(Guid.NewGuid())));
                    return await _fixture.ExchangeExistsAsync(exchangeName);
                },
                "the exchange is declared again on the replacement channel");
        }

        /// <summary>
        /// A guard for the concurrency the publish path has to survive: one shared channel, one shared
        /// exchange-declared cache, many publishers, several exchanges.
        /// </summary>
        [Fact]
        public async Task ConcurrentPublishesAcrossSeveralExchanges_AllArriveExactlyOnce()
        {
            const int perType = 300;

            var (services, bus) = BusTestHost.Create(_fixture);
            await using var _ = services;
            await using var __ = bus;

            var firstReceived = new ConcurrentBag<Guid>();
            var secondReceived = new ConcurrentBag<Guid>();

            await bus.SubscribeAsync<FirstPathEvent>("publishpath.concurrent.first.queue",
                msg => { firstReceived.Add(msg.CorrelationId); return Task.CompletedTask; });
            await bus.SubscribeAsync<SecondPathEvent>("publishpath.concurrent.second.queue",
                msg => { secondReceived.Add(msg.CorrelationId); return Task.CompletedTask; });

            var sentFirst = new ConcurrentBag<Guid>();
            var sentSecond = new ConcurrentBag<Guid>();
            var failures = new ConcurrentBag<Exception>();

            await Parallel.ForEachAsync(
                Enumerable.Range(0, perType * 2),
                new ParallelOptions { MaxDegreeOfParallelism = 32 },
                async (index, cancellationToken) =>
                {
                    try
                    {
                        if (index % 2 == 0)
                        {
                            var message = new FirstPathEvent(Guid.NewGuid());
                            await bus.PublishAsync(message, cancellationToken);
                            sentFirst.Add(message.CorrelationId);
                        }
                        else
                        {
                            var message = new SecondPathEvent(Guid.NewGuid());
                            await bus.PublishAsync(message, cancellationToken);
                            sentSecond.Add(message.CorrelationId);
                        }
                    }
                    catch (Exception ex)
                    {
                        failures.Add(ex);
                    }
                });

            Assert.Empty(failures);
            Assert.Equal(perType, sentFirst.Count);
            Assert.Equal(perType, sentSecond.Count);

            await Wait.UntilAsync(
                () => firstReceived.Count >= perType && secondReceived.Count >= perType,
                "every concurrently published message is handled",
                TimeSpan.FromSeconds(60));

            Assert.Equal(perType, firstReceived.Distinct().Count());
            Assert.Equal(perType, secondReceived.Distinct().Count());
            Assert.Empty(sentFirst.Except(firstReceived));
            Assert.Empty(sentSecond.Except(secondReceived));
        }
    }
}
