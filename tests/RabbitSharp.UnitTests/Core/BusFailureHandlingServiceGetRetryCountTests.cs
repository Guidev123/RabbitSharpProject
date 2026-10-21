using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitSharp.Abstractions;
using RabbitSharp.Core;
using System.Text;
using Xunit;

namespace RabbitSharp.UnitTests.Core
{
    public class BusFailureHandlingServiceGetRetryCountTests
    {
        private readonly IBusFailureHandlingService _sut =
            new BusFailureHandlingService(NullLogger<BusFailureHandlingService>.Instance, new SimpleNamingConventions());

        private static BasicProperties PropertiesWithHeader(string key, object? value) => new()
        {
            Headers = new Dictionary<string, object?> { [key] = value }
        };

        [Fact]
        public void GetRetryCount_NullProperties_ReturnsZero()
        {
            Assert.Equal(0, _sut.GetRetryCount(null));
        }

        [Fact]
        public void GetRetryCount_NoHeaders_ReturnsZero()
        {
            var properties = new BasicProperties();

            Assert.Equal(0, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_MissingHeader_ReturnsZero()
        {
            var properties = PropertiesWithHeader("some-other-header", 5);

            Assert.Equal(0, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_IntHeader_ReturnsValue()
        {
            var properties = PropertiesWithHeader("x-retry-count", 3);

            Assert.Equal(3, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_LongHeader_ReturnsValue()
        {
            // A non-.NET producer (or the AMQP wire format itself) may encode the counter as a
            // 64-bit integer.
            var properties = PropertiesWithHeader("x-retry-count", 4L);

            Assert.Equal(4, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_ShortHeader_ReturnsValue()
        {
            var properties = PropertiesWithHeader("x-retry-count", (short)2);

            Assert.Equal(2, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_ByteHeader_ReturnsValue()
        {
            var properties = PropertiesWithHeader("x-retry-count", (byte)1);

            Assert.Equal(1, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_UIntHeader_ReturnsValue()
        {
            var properties = PropertiesWithHeader("x-retry-count", 6u);

            Assert.Equal(6, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_StringHeader_ParsesValue()
        {
            // e.g. a header edited by hand through the RabbitMQ Management UI.
            var properties = PropertiesWithHeader("x-retry-count", "7");

            Assert.Equal(7, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_Utf8ByteArrayHeader_ParsesValue()
        {
            // The AMQP client surfaces longstr header values as UTF-8 bytes, not raw int32.
            var properties = PropertiesWithHeader("x-retry-count", Encoding.UTF8.GetBytes("9"));

            Assert.Equal(9, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_UnparseableByteArrayHeader_ReturnsZero()
        {
            var properties = PropertiesWithHeader("x-retry-count", new byte[] { 0xFF, 0xFE });

            Assert.Equal(0, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_UnsupportedType_ReturnsZero()
        {
            var properties = PropertiesWithHeader("x-retry-count", 3.14);

            Assert.Equal(0, _sut.GetRetryCount(properties));
        }

        [Fact]
        public void GetRetryCount_NegativeLongHeader_ClampsToZero()
        {
            var properties = PropertiesWithHeader("x-retry-count", -5L);

            Assert.Equal(0, _sut.GetRetryCount(properties));
        }
    }
}
