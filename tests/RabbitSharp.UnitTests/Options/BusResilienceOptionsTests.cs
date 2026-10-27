using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.Options
{
    public class BusResilienceOptionsTests
    {
        [Fact]
        public void Validate_WithDefaults_DoesNotThrow()
        {
            var options = new BusResilienceOptions();

            var exception = Record.Exception(() => options.Validate());

            Assert.Null(exception);
        }

        [Fact]
        public void Strategy_DefaultsToInMemory()
        {
            var options = new BusResilienceOptions();

            Assert.Equal(RetryStrategy.InMemory, options.Strategy);
        }

        [Fact]
        public void Validate_WithNegativeMaxDeliveryRetryAttempts_Throws()
        {
            var options = new BusResilienceOptions { MaxDeliveryRetryAttempts = -1 };

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithZeroMaxDeliveryRetryAttempts_DoesNotThrow()
        {
            var options = new BusResilienceOptions { MaxDeliveryRetryAttempts = 0 };

            var exception = Record.Exception(() => options.Validate());

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_WithNonPositiveInitialDeliveryRetryDelay_Throws()
        {
            var options = new BusResilienceOptions { InitialDeliveryRetryDelay = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithMaxDelayLessThanInitialDelay_Throws()
        {
            var options = new BusResilienceOptions
            {
                InitialDeliveryRetryDelay = TimeSpan.FromSeconds(10),
                MaxDeliveryRetryDelay = TimeSpan.FromSeconds(5)
            };

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithMaxDelayAtInt32MillisecondOverflow_Throws()
        {
            var options = new BusResilienceOptions
            {
                InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1),
                MaxDeliveryRetryDelay = TimeSpan.FromMilliseconds(int.MaxValue)
            };

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithMaxDelayJustBelowInt32MillisecondOverflow_DoesNotThrow()
        {
            // RetryQueue, not the InMemory default: at this magnitude the worst-case in-memory
            // window check would throw first, which is not what this test is about.
            var options = new BusResilienceOptions
            {
                Strategy = RetryStrategy.RetryQueue,
                InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1),
                MaxDeliveryRetryDelay = TimeSpan.FromMilliseconds(int.MaxValue - 1000)
            };

            var exception = Record.Exception(() => options.Validate());

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_WithNonPositiveMaxInMemoryRetryWindow_Throws()
        {
            var options = new BusResilienceOptions { MaxInMemoryRetryWindow = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_InMemoryStrategy_WithWorstCaseBackoffUnderTheWindow_DoesNotThrow()
        {
            var options = new BusResilienceOptions
            {
                Strategy = RetryStrategy.InMemory,
                MaxDeliveryRetryAttempts = 3,
                InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1),
                MaxDeliveryRetryDelay = TimeSpan.FromSeconds(30),
                MaxInMemoryRetryWindow = TimeSpan.FromMinutes(15)
            };

            var exception = Record.Exception(() => options.Validate());

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_InMemoryStrategy_WithWorstCaseBackoffOverTheWindow_Throws()
        {
            var options = new BusResilienceOptions
            {
                Strategy = RetryStrategy.InMemory,
                MaxDeliveryRetryAttempts = 10,
                InitialDeliveryRetryDelay = TimeSpan.FromMinutes(1),
                MaxDeliveryRetryDelay = TimeSpan.FromMinutes(10),
                MaxInMemoryRetryWindow = TimeSpan.FromMinutes(15)
            };

            var exception = Assert.Throws<ArgumentException>(() => options.Validate());
            Assert.Equal("Strategy", exception.ParamName);
        }

        [Fact]
        public void Validate_RetryQueueStrategy_IgnoresTheInMemoryWindowEvenWhenItWouldBeExceeded()
        {
            var options = new BusResilienceOptions
            {
                Strategy = RetryStrategy.RetryQueue,
                MaxDeliveryRetryAttempts = 10,
                InitialDeliveryRetryDelay = TimeSpan.FromMinutes(1),
                MaxDeliveryRetryDelay = TimeSpan.FromMinutes(10),
                MaxInMemoryRetryWindow = TimeSpan.FromMinutes(15)
            };

            var exception = Record.Exception(() => options.Validate());

            Assert.Null(exception);
        }

        [Theory]
        [InlineData(1, 1000)]
        [InlineData(2, 2000)]
        [InlineData(3, 4000)]
        [InlineData(4, 8000)]
        public void GetRetryDelayMilliseconds_DoublesPerAttempt(int attempt, int expectedMs)
        {
            var options = new BusResilienceOptions
            {
                InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1),
                MaxDeliveryRetryDelay = TimeSpan.FromSeconds(300)
            };

            Assert.Equal(expectedMs, options.GetRetryDelayMilliseconds(attempt));
        }

        [Fact]
        public void GetRetryDelayMilliseconds_IsCappedByMaxDelay()
        {
            var options = new BusResilienceOptions
            {
                InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1),
                MaxDeliveryRetryDelay = TimeSpan.FromSeconds(5)
            };

            Assert.Equal(5000, options.GetRetryDelayMilliseconds(10));
        }
    }
}
