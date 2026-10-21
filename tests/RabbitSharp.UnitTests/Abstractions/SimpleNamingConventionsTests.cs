using RabbitSharp.Abstractions;
using RabbitSharp.Enums;
using Xunit;

namespace RabbitSharp.UnitTests.Abstractions
{
    public class SimpleNamingConventionsTests
    {
        private sealed record OrderCreated(Guid CorrelationId) : IMessage;

        private readonly SimpleNamingConventions _sut = new();

        [Fact]
        public void ExchangeNamingConvention_ReturnsLowercaseInvariantTypeName()
        {
            var name = _sut.ExchangeNamingConvention(typeof(OrderCreated));

            Assert.Equal("ordercreated", name);
        }

        [Theory]
        [InlineData(ExchangeTypeEnum.Topic)]
        [InlineData(ExchangeTypeEnum.Direct)]
        public void RoutingKeyNamingConvention_ReturnsConcreteKeyWithoutWildcards(ExchangeTypeEnum exchangeType)
        {
            var key = _sut.RoutingKeyNamingConvention(typeof(OrderCreated), exchangeType);

            Assert.Equal("OrderCreated", key);
        }

        [Fact]
        public void RoutingKeyNamingConvention_Fanout_ReturnsEmpty()
        {
            var key = _sut.RoutingKeyNamingConvention(typeof(OrderCreated), ExchangeTypeEnum.Fanout);

            Assert.Equal(string.Empty, key);
        }

        [Fact]
        public void BindingPatternNamingConvention_Topic_ReturnsWildcardPattern()
        {
            var pattern = _sut.BindingPatternNamingConvention(typeof(OrderCreated), ExchangeTypeEnum.Topic);

            Assert.Equal("OrderCreated.#", pattern);
        }

        [Fact]
        public void BindingPatternNamingConvention_Direct_ReturnsTypeName()
        {
            var pattern = _sut.BindingPatternNamingConvention(typeof(OrderCreated), ExchangeTypeEnum.Direct);

            Assert.Equal("OrderCreated", pattern);
        }

        [Fact]
        public void BindingPatternNamingConvention_Fanout_ReturnsEmpty()
        {
            var pattern = _sut.BindingPatternNamingConvention(typeof(OrderCreated), ExchangeTypeEnum.Fanout);

            Assert.Equal(string.Empty, pattern);
        }
    }
}
