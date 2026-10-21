using RabbitSharp.Core;
using Xunit;

namespace RabbitSharp.UnitTests.Core
{
    public class ConnectionManagerParseHostAndPortTests
    {
        [Fact]
        public void ParseHostAndPort_BareHostname_UsesDefaultPort()
        {
            var (host, port) = ConnectionManager.ParseHostAndPort("localhost", 5672);

            Assert.Equal("localhost", host);
            Assert.Equal(5672, port);
        }

        [Fact]
        public void ParseHostAndPort_HostWithPort_ParsesBoth()
        {
            var (host, port) = ConnectionManager.ParseHostAndPort("broker.internal:5673", -1);

            Assert.Equal("broker.internal", host);
            Assert.Equal(5673, port);
        }

        [Fact]
        public void ParseHostAndPort_IPv4WithPort_ParsesBoth()
        {
            var (host, port) = ConnectionManager.ParseHostAndPort("10.0.0.5:5671", -1);

            Assert.Equal("10.0.0.5", host);
            Assert.Equal(5671, port);
        }

        [Fact]
        public void ParseHostAndPort_BracketedIPv6WithoutPort_StripsBrackets()
        {
            var (host, port) = ConnectionManager.ParseHostAndPort("[::1]", 5672);

            Assert.Equal("::1", host);
            Assert.Equal(5672, port);
        }

        [Fact]
        public void ParseHostAndPort_BracketedIPv6WithPort_ParsesBoth()
        {
            var (host, port) = ConnectionManager.ParseHostAndPort("[::1]:5673", -1);

            Assert.Equal("::1", host);
            Assert.Equal(5673, port);
        }

        [Fact]
        public void ParseHostAndPort_BareIPv6WithoutBrackets_PassesThroughUntouched()
        {
            var (host, port) = ConnectionManager.ParseHostAndPort("2001:db8::1", 5672);

            Assert.Equal("2001:db8::1", host);
            Assert.Equal(5672, port);
        }
    }
}
