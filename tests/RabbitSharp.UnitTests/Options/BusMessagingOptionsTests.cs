using RabbitSharp.Enums;
using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.Options
{
    public class BusMessagingOptionsTests
    {
        [Fact]
        public void Defaults_RejectEmptyCorrelationId()
        {
            // A payload whose property names did not bind deserializes to an all-defaults instance
            // instead of throwing. Left handed to the handler, it would be acked as processed; this
            // default is what sends it to the dead-letter queue instead.
            Assert.True(new BusMessagingOptions().RejectMessagesWithEmptyCorrelationId);
        }

        [Fact]
        public void Defaults_SerializerIsCaseInsensitive()
        {
            // So a camelCase payload from a non-.NET producer binds to its properties instead of
            // silently deserializing into an all-defaults instance.
            Assert.True(new BusMessagingOptions().SerializerOptions.PropertyNameCaseInsensitive);
        }

        [Fact]
        public void Defaults_ExchangeTypeIsTopic()
        {
            Assert.Equal(ExchangeTypeEnum.Topic, new BusMessagingOptions().DefaultExchangeType);
        }

        [Fact]
        public void Validate_WithDefaults_DoesNotThrow()
        {
            var exception = Record.Exception(() => new BusMessagingOptions().Validate());

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_WithNullSerializerOptions_Throws()
        {
            var options = new BusMessagingOptions { SerializerOptions = null! };

            var ex = Assert.Throws<ArgumentException>(() => options.Validate());

            Assert.Equal(nameof(BusMessagingOptions.SerializerOptions), ex.ParamName);
        }

        [Fact]
        public void Validate_WithUndefinedExchangeType_Throws()
        {
            var options = new BusMessagingOptions { DefaultExchangeType = (ExchangeTypeEnum)999 };

            var ex = Assert.Throws<ArgumentException>(() => options.Validate());

            Assert.Equal(nameof(BusMessagingOptions.DefaultExchangeType), ex.ParamName);
        }
    }
}
