using RabbitMQ.Client;
using RabbitSharp.Core;
using Xunit;

namespace RabbitSharp.UnitTests.Core
{
    public class BusFailureHandlingServiceRepublishPropertiesTests
    {
        private static BasicProperties Produced() => new()
        {
            MessageId = "11111111-1111-1111-1111-111111111111",
            ContentType = "application/json",
            ContentEncoding = "utf-8",
            CorrelationId = "saga-4711",
            ReplyTo = "orders.reply",
            Type = "OrderCreated.v2",
            AppId = "orders-api",
            ClusterId = "cluster-a",
            Priority = 5,
            Timestamp = new AmqpTimestamp(1700000000),
            Headers = new Dictionary<string, object?> { ["x-tenant"] = "acme" }
        };

        private static Dictionary<string, object?> Headers() => new() { ["x-retry-count"] = 1 };

        [Fact]
        public void BuildRepublishProperties_CarriesEveryProducerProperty()
        {
            var source = Produced();

            var republished = BusFailureHandlingService.BuildRepublishProperties(source, Headers());

            Assert.Equal(source.MessageId, republished.MessageId);
            Assert.Equal(source.ContentType, republished.ContentType);
            Assert.Equal(source.ContentEncoding, republished.ContentEncoding);
            Assert.Equal(source.CorrelationId, republished.CorrelationId);
            Assert.Equal(source.ReplyTo, republished.ReplyTo);
            Assert.Equal(source.Type, republished.Type);
            Assert.Equal(source.AppId, republished.AppId);
            Assert.Equal(source.ClusterId, republished.ClusterId);
            Assert.Equal(source.Priority, republished.Priority);
            Assert.Equal(source.Timestamp.UnixTime, republished.Timestamp.UnixTime);
        }

        [Fact]
        public void BuildRepublishProperties_ReplacesTheHeaderTableWithTheGivenOne()
        {
            var republished = BusFailureHandlingService.BuildRepublishProperties(Produced(), Headers());

            Assert.Equal(1, republished.Headers!["x-retry-count"]);
            Assert.False(republished.Headers!.ContainsKey("x-tenant"));
        }

        [Fact]
        public void BuildRepublishProperties_ForcesPersistentEvenForATransientOriginal()
        {
            var source = Produced();
            source.Persistent = false;

            var republished = BusFailureHandlingService.BuildRepublishProperties(source, Headers());

            Assert.True(republished.Persistent);
            Assert.Equal(DeliveryModes.Persistent, republished.DeliveryMode);
        }

        /// <summary>
        /// A per-message TTL competes with the retry queue's own fixed x-message-ttl - the shorter of
        /// the two wins, so it would cut the backoff short - and on the dead-letter queue, which has
        /// no dead-letter exchange of its own, it would expire the message and the broker would drop
        /// it.
        /// </summary>
        [Fact]
        public void BuildRepublishProperties_DropsTheProducersExpiration()
        {
            var source = Produced();
            source.Expiration = "60000";

            var republished = BusFailureHandlingService.BuildRepublishProperties(source, Headers());

            Assert.Null(republished.Expiration);
        }

        /// <summary>
        /// The broker validates user-id against the authenticated user of the publishing connection
        /// and answers a mismatch with 406 PRECONDITION_FAILED on basic.publish, which closes the
        /// channel. The connection republishing is the consumer's, not the producer's.
        /// </summary>
        [Fact]
        public void BuildRepublishProperties_DropsTheProducersUserId()
        {
            var source = Produced();
            source.UserId = "some-other-service";

            var republished = BusFailureHandlingService.BuildRepublishProperties(source, Headers());

            Assert.Null(republished.UserId);
        }

        /// <summary>
        /// Used only for a RetryStrategy.RetryQueue publish: the delay is carried per message instead
        /// of as a fixed queue argument, so the retry queue can be redeclared after the configured
        /// delay changes without the broker rejecting it with 406 PRECONDITION_FAILED.
        /// </summary>
        [Fact]
        public void BuildRepublishProperties_WithExpirationMs_SetsExpirationToThatDelay()
        {
            var source = Produced();
            source.Expiration = "60000";

            var republished = BusFailureHandlingService.BuildRepublishProperties(source, Headers(), expirationMs: 2000);

            Assert.Equal("2000", republished.Expiration);
        }

        [Fact]
        public void BuildRepublishProperties_WithAnEmptyOriginal_CarriesOnlyWhatItSets()
        {
            var republished = BusFailureHandlingService.BuildRepublishProperties(new BasicProperties(), Headers());

            Assert.Null(republished.ContentType);
            Assert.Null(republished.CorrelationId);
            Assert.Null(republished.ReplyTo);
            Assert.True(republished.Persistent);
            Assert.Equal(1, republished.Headers!["x-retry-count"]);
        }
    }
}
