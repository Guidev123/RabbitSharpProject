using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.Options
{
    public class BrokerOptionsTests
    {
        private static BrokerOptions ValidOptions() => new()
        {
            Hosts = new[] { "localhost" },
            Username = "guest",
            Password = "guest"
        };

        [Fact]
        public void Validate_WithValidOptions_DoesNotThrow()
        {
            var options = ValidOptions();

            var exception = Record.Exception(() => options.Validate());

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_WithNullHosts_Throws()
        {
            var options = ValidOptions();
            options.Hosts = null!;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithEmptyHosts_Throws()
        {
            var options = ValidOptions();
            options.Hosts = Array.Empty<string>();

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithBlankHostEntry_Throws()
        {
            var options = ValidOptions();
            options.Hosts = new[] { "localhost", "   " };

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithMissingUsername_Throws()
        {
            var options = ValidOptions();
            options.Username = null!;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithNullPassword_Throws()
        {
            var options = ValidOptions();
            options.Password = null!;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithEmptyPassword_DoesNotThrow()
        {
            var options = ValidOptions();
            options.Password = string.Empty;

            var exception = Record.Exception(() => options.Validate());

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_WithNegativeTryConnectMaxRetries_Throws()
        {
            var options = ValidOptions();
            options.TryConnectMaxRetries = -1;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithZeroTryConnectMaxRetries_DoesNotThrow()
        {
            var options = ValidOptions();
            options.TryConnectMaxRetries = 0;

            var exception = Record.Exception(() => options.Validate());

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_WithNegativeNetworkRecoveryInterval_Throws()
        {
            var options = ValidOptions();
            options.NetworkRecoveryIntervalInSeconds = -1;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithNegativeHeartbeatInterval_Throws()
        {
            var options = ValidOptions();
            options.HeartbeatIntervalSeconds = -1;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithNonPositiveMaxConnectRetryDelay_Throws()
        {
            var options = ValidOptions();
            options.MaxConnectRetryDelay = TimeSpan.Zero;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithNonPositiveAutomaticRecoveryWaitTimeout_Throws()
        {
            var options = ValidOptions();
            options.AutomaticRecoveryWaitTimeout = TimeSpan.Zero;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithZeroPrefetchCount_Throws()
        {
            var options = ValidOptions();
            options.PrefetchCount = 0;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }

        [Fact]
        public void Validate_WithZeroConsumerDispatchConcurrency_Throws()
        {
            var options = ValidOptions();
            options.ConsumerDispatchConcurrency = 0;

            Assert.Throws<ArgumentException>(() => options.Validate());
        }
    }
}
