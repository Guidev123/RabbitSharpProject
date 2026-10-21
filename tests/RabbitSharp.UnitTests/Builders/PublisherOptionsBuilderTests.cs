using RabbitSharp.Builders;
using RabbitSharp.Enums;
using Xunit;

namespace RabbitSharp.UnitTests.Builders
{
    public class PublisherOptionsBuilderTests
    {
        [Fact]
        public void Build_WithoutExchangeName_Throws()
        {
            var builder = new PublisherOptionsBuilder()
                .WithRoutingKey("some.key");

            Assert.Throws<InvalidOperationException>(() => builder.Build());
        }

        [Fact]
        public void Build_TopicExchangeWithoutRoutingKey_Throws()
        {
            var builder = new PublisherOptionsBuilder()
                .WithExchange(e =>
                {
                    e.WithName("orders");
                    e.WithType(ExchangeTypeEnum.Topic);
                });

            Assert.Throws<InvalidOperationException>(() => builder.Build());
        }

        [Fact]
        public void Build_DirectExchangeWithoutRoutingKey_Throws()
        {
            var builder = new PublisherOptionsBuilder()
                .WithExchange(e =>
                {
                    e.WithName("orders");
                    e.WithType(ExchangeTypeEnum.Direct);
                });

            Assert.Throws<InvalidOperationException>(() => builder.Build());
        }

        [Fact]
        public void Build_FanoutExchangeWithoutRoutingKey_DoesNotThrowAndUsesEmptyKey()
        {
            var builder = new PublisherOptionsBuilder()
                .WithExchange(e =>
                {
                    e.WithName("broadcasts");
                    e.WithType(ExchangeTypeEnum.Fanout);
                });

            var options = builder.Build();

            Assert.Equal(string.Empty, options.RoutingKey);
        }

        [Fact]
        public void WithHeader_AddsToHeadersDictionary()
        {
            var options = new PublisherOptionsBuilder()
                .WithExchange(e =>
                {
                    e.WithName("orders");
                    e.WithType(ExchangeTypeEnum.Fanout);
                })
                .WithHeader("x-custom", "value")
                .Build();

            Assert.NotNull(options.Headers);
            Assert.Equal("value", options.Headers!["x-custom"]);
        }
    }
}
