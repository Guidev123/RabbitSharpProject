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
            var options = new BusResilienceOptions
            {
                InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1),
                MaxDeliveryRetryDelay = TimeSpan.FromMilliseconds(int.MaxValue - 1000)
            };

            var exception = Record.Exception(() => options.Validate());

            Assert.Null(exception);
        }
    }
}
