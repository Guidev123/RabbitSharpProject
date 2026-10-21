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

        [Fact]
        public void RoutingKeyNamingConvention_Topic_ReturnsWildcardPattern()
        {
            var key = _sut.RoutingKeyNamingConvention(typeof(OrderCreated), ExchangeTypeEnum.Topic);

            Assert.Equal("OrderCreated.#", key);
        }

        [Fact]
        public void RoutingKeyNamingConvention_Direct_ReturnsTypeName()
        {
            var key = _sut.RoutingKeyNamingConvention(typeof(OrderCreated), ExchangeTypeEnum.Direct);

            Assert.Equal("OrderCreated", key);
        }

        [Fact]
        public void RoutingKeyNamingConvention_Fanout_ReturnsEmpty()
        {
            var key = _sut.RoutingKeyNamingConvention(typeof(OrderCreated), ExchangeTypeEnum.Fanout);

            Assert.Equal(string.Empty, key);
        }
    }
}
