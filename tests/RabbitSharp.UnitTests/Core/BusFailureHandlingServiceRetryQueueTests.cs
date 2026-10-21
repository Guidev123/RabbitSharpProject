using RabbitSharp.Core;
using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.Core
{
    public class BusFailureHandlingServiceRetryQueueTests
    {
        private static BusResilienceOptions Resilience(int attempts, double initialSeconds, double maxSeconds) => new()
        {
            MaxDeliveryRetryAttempts = attempts,
            InitialDeliveryRetryDelay = TimeSpan.FromSeconds(initialSeconds),
            MaxDeliveryRetryDelay = TimeSpan.FromSeconds(maxSeconds)
        };

        [Theory]
        [InlineData(1, 1000)]
        [InlineData(2, 2000)]
        [InlineData(3, 4000)]
        [InlineData(4, 8000)]
        public void GetRetryDelayMilliseconds_DoublesPerAttempt(int attempt, int expectedMs)
        {
            var delay = BusFailureHandlingService.GetRetryDelayMilliseconds(attempt, Resilience(5, 1, 300));

            Assert.Equal(expectedMs, delay);
        }

        [Fact]
        public void GetRetryDelayMilliseconds_IsCappedByMaxDelay()
        {
            var delay = BusFailureHandlingService.GetRetryDelayMilliseconds(10, Resilience(10, 1, 5));

            Assert.Equal(5000, delay);
        }

        [Fact]
        public void GetRetryDelaysMilliseconds_OneEntryPerDistinctDelay()
        {
            var delays = BusFailureHandlingService.GetRetryDelaysMilliseconds(Resilience(3, 1, 300));

            Assert.Equal(new[] { 1000, 2000, 4000 }, delays);
        }

        [Fact]
        public void GetRetryDelaysMilliseconds_CollapsesAttemptsThatShareTheCappedDelay()
        {
            var delays = BusFailureHandlingService.GetRetryDelaysMilliseconds(Resilience(6, 1, 4));

            Assert.Equal(new[] { 1000, 2000, 4000 }, delays);
        }

        [Fact]
        public void GetRetryDelaysMilliseconds_NoRetries_DeclaresNoRetryQueue()
        {
            var delays = BusFailureHandlingService.GetRetryDelaysMilliseconds(Resilience(0, 1, 300));

            Assert.Empty(delays);
        }

        [Fact]
        public void GetRetryQueueName_CarriesTheFixedTtlOfTheQueue()
        {
            var name = BusFailureHandlingService.GetRetryQueueName("orders.retry", 2000);

            Assert.Equal("orders.retry.2000ms", name);
        }

        [Fact]
        public void GetRetryQueueName_AttemptsWithTheSameDelayShareOneQueue()
        {
            var resilience = Resilience(6, 1, 4);

            var fifth = BusFailureHandlingService.GetRetryQueueName(
                "orders.retry", BusFailureHandlingService.GetRetryDelayMilliseconds(5, resilience));
            var sixth = BusFailureHandlingService.GetRetryQueueName(
                "orders.retry", BusFailureHandlingService.GetRetryDelayMilliseconds(6, resilience));

            Assert.Equal(fifth, sixth);
        }
    }
}
