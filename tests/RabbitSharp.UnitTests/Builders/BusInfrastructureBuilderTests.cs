using RabbitSharp.Builders;
using RabbitSharp.Core;
using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.Builders
{
    public class BusInfrastructureBuilderTests
    {
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void Constructor_WithBlankQueueName_Throws(string? queueName)
        {
            // ThrowsAny, not Throws: a null name is an ArgumentNullException and an empty or
            // whitespace one an ArgumentException, and both are what a caller guards against.
            var exception = Assert.ThrowsAny<ArgumentException>(() => new BusInfrastructureBuilder(queueName!));

            Assert.Equal("queueName", exception.ParamName);
        }

        [Fact]
        public void Constructor_DerivesTheRetryAndDeadLetterNamesFromTheBaseName()
        {
            var options = new BusInfrastructureBuilder("orders").Build();

            Assert.Equal("orders", options.MainQueue.QueueName);
            Assert.Equal("orders.retry", options.RetryQueue.QueueName);
            Assert.Equal("orders.deadletter", options.DeadLetterQueue.QueueName);
        }

        /// <summary>
        /// x-expires counts a queue as unused when nothing consumes from it, redeclares it or reads it
        /// with basic.get - and publishing to it does not count. A retry queue is only ever published
        /// to, so an expiring one is deleted while the subscription is still healthy, taking any
        /// message waiting out its TTL with it and leaving every later retry to fail to route.
        /// </summary>
        [Fact]
        public void ForDevelopment_ExpiresTheMainQueueButNeverTheRetryQueue()
        {
            var options = new BusInfrastructureBuilder("orders").ForDevelopment().Build();

            Assert.True(options.MainQueue.Arguments.ContainsKey("x-expires"));
            Assert.False(options.RetryQueue.Arguments.ContainsKey("x-expires"));
            Assert.False(options.DeadLetterQueue.Arguments.ContainsKey("x-expires"));
        }

        /// <summary>
        /// The per-delay retry queues are declared from the retry queue template, so an x-expires on
        /// the template reaches every one of them.
        /// </summary>
        [Fact]
        public void ForDevelopment_RetryQueueArgumentsCarryNoExpiryIntoTheDeclaredQueues()
        {
            var options = new BusInfrastructureBuilder("orders").ForDevelopment().Build();
            var resilience = new BusResilienceOptions
            {
                MaxDeliveryRetryAttempts = 3,
                InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1),
                MaxDeliveryRetryDelay = TimeSpan.FromMinutes(5)
            };

            foreach (var delayMs in BusFailureHandlingService.GetRetryDelaysMilliseconds(resilience))
            {
                var declared = new Dictionary<string, object?>(options.RetryQueue.Arguments)
                {
                    ["x-message-ttl"] = delayMs
                };

                Assert.False(
                    declared.ContainsKey("x-expires"),
                    $"The retry queue for {delayMs}ms would be declared with an expiry.");
            }
        }

        [Fact]
        public void ForDevelopment_KeepsTheQueueNamesItWasBuiltWith()
        {
            var options = new BusInfrastructureBuilder("orders").ForDevelopment().Build();

            options.Validate(new BusResilienceOptions { MaxDeliveryRetryAttempts = 3 });
        }
    }
}
