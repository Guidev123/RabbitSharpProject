using RabbitSharp.Enums;
using RabbitSharp.Extensions;
using Xunit;

namespace RabbitSharp.UnitTests.Extensions
{
    public class EnumDescriptionExtensionsTests
    {
        [Theory]
        [InlineData(ExchangeTypeEnum.Direct, "direct")]
        [InlineData(ExchangeTypeEnum.Fanout, "fanout")]
        [InlineData(ExchangeTypeEnum.Topic, "topic")]
        public void GetEnumDescription_ExchangeType_ReturnsAmqpTypeString(ExchangeTypeEnum value, string expected)
        {
            Assert.Equal(expected, value.GetEnumDescription());
        }

        [Theory]
        [InlineData(OverFlowStrategyEnum.RejectPublish, "reject-publish")]
        [InlineData(OverFlowStrategyEnum.DropHead, "drop-head")]
        public void GetEnumDescription_OverflowStrategy_ReturnsAmqpArgumentString(OverFlowStrategyEnum value, string expected)
        {
            Assert.Equal(expected, value.GetEnumDescription());
        }

        [Fact]
        public void GetEnumDescription_UnnamedValue_FallsBackToToString()
        {
            var unnamed = (ExchangeTypeEnum)999;

            var description = unnamed.GetEnumDescription();

            Assert.Equal(unnamed.ToString(), description);
        }

        [Fact]
        public void GetEnumDescription_CalledTwice_ReturnsSameValue()
        {
            var first = ExchangeTypeEnum.Topic.GetEnumDescription();
            var second = ExchangeTypeEnum.Topic.GetEnumDescription();

            Assert.Equal(first, second);
        }
    }
}
