using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.Options
{
    public class BrokerOptionsValidateTests
    {
        private static BrokerOptions Valid(params string[] hosts) => new()
        {
            Hosts = hosts.Length == 0 ? new[] { "localhost" } : hosts,
            Username = "guest",
            Password = "guest"
        };

        [Fact]
        public void Validate_WellFormedOptions_DoesNotThrow()
        {
            Valid("localhost", "broker.internal:5673", "[::1]:5671").Validate();
        }

        [Theory]
        [InlineData(0)]
        [InlineData(65536)]
        [InlineData(-2)]
        public void Validate_PortOutOfRange_Throws(int port)
        {
            var options = Valid();
            options.Port = port;

            var ex = Assert.Throws<ArgumentException>(() => options.Validate());

            Assert.Equal(nameof(BrokerOptions.Port), ex.ParamName);
        }

        [Fact]
        public void Validate_ProtocolDefaultPort_DoesNotThrow()
        {
            var options = Valid();
            options.Port = -1;

            options.Validate();
        }

        [Theory]
        [InlineData("localhost:99999")]
        [InlineData("localhost:0")]
        [InlineData("localhost:")]
        [InlineData("localhost:amqp")]
        [InlineData("[::1]:99999")]
        public void Validate_MalformedHostEntry_ThrowsInsteadOfFailingLaterAsDns(string host)
        {
            var options = Valid(host);

            var ex = Assert.Throws<ArgumentException>(() => options.Validate());

            Assert.Equal(nameof(BrokerOptions.Hosts), ex.ParamName);
            Assert.Contains(host, ex.Message);
        }

        [Fact]
        public void Validate_NegativeRecoveryFailFastWindow_Throws()
        {
            var options = Valid();
            options.RecoveryFailFastWindow = TimeSpan.FromSeconds(-1);

            var ex = Assert.Throws<ArgumentException>(() => options.Validate());

            Assert.Equal(nameof(BrokerOptions.RecoveryFailFastWindow), ex.ParamName);
        }

        [Fact]
        public void Validate_ZeroRecoveryFailFastWindow_DoesNotThrow()
        {
            var options = Valid();
            options.RecoveryFailFastWindow = TimeSpan.Zero;

            options.Validate();
        }
    }
}
