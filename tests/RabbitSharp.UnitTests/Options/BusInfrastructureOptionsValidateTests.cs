using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.Options
{
    public class BusInfrastructureOptionsValidateTests
    {
        private static BusInfrastructureOptions Valid()
        {
            var options = new BusInfrastructureOptions();
            options.MainQueue.QueueName = "orders";
            options.RetryQueue.QueueName = "orders.retry";
            options.DeadLetterQueue.QueueName = "orders.deadletter";
            return options;
        }

        private static BusResilienceOptions Resilience(int attempts) => new()
        {
            MaxDeliveryRetryAttempts = attempts,
            Strategy = RetryStrategy.RetryQueue
        };

        [Fact]
        public void Validate_WithEveryNameSet_DoesNotThrow()
        {
            Valid().Validate(Resilience(3));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void Validate_WithBlankMainQueueName_Throws(string? queueName)
        {
            var options = Valid();
            options.MainQueue.QueueName = queueName!;

            var exception = Assert.Throws<ArgumentException>(() => options.Validate(Resilience(3)));

            Assert.Equal("MainQueue", exception.ParamName);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void Validate_WithBlankDeadLetterQueueName_Throws(string? queueName)
        {
            var options = Valid();
            options.DeadLetterQueue.QueueName = queueName!;

            var exception = Assert.Throws<ArgumentException>(() => options.Validate(Resilience(3)));

            Assert.Equal("DeadLetterQueue", exception.ParamName);
        }

        [Fact]
        public void Validate_WithBlankRetryQueueNameAndRetriesConfigured_Throws()
        {
            var options = Valid();
            options.RetryQueue.QueueName = string.Empty;

            var exception = Assert.Throws<ArgumentException>(() => options.Validate(Resilience(1)));

            Assert.Equal("RetryQueue", exception.ParamName);
        }

        [Fact]
        public void Validate_WithBlankRetryQueueNameAndNoRetries_DoesNotThrow()
        {
            // With no retry attempts no retry queue is declared and the name is never published to,
            // so requiring it would reject a configuration that works.
            var options = Valid();
            options.RetryQueue.QueueName = string.Empty;

            options.Validate(Resilience(0));
        }

        [Fact]
        public void Validate_WithBlankRetryQueueNameAndInMemoryStrategy_DoesNotThrow()
        {
            // RetryStrategy.InMemory never declares or publishes to a retry queue, regardless of how
            // many attempts are configured, so its name is irrelevant.
            var options = Valid();
            options.RetryQueue.QueueName = string.Empty;

            var resilience = new BusResilienceOptions { MaxDeliveryRetryAttempts = 3, Strategy = RetryStrategy.InMemory };

            var exception = Record.Exception(() => options.Validate(resilience));

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_WithNullResilienceOptions_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => Valid().Validate(null!));
        }
    }
}
