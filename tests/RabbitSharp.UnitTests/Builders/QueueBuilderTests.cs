using RabbitSharp.Builders;
using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.Builders
{
    public class QueueBuilderTests
    {
        private static QueueBuilder NewBuilder() => new(new QueueOptions());

        [Fact]
        public void WithMaxLength_Negative_Throws()
        {
            var builder = NewBuilder();

            Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithMaxLength(-1));
        }

        [Fact]
        public void WithMaxLength_Zero_DoesNotThrow()
        {
            var builder = NewBuilder();

            var exception = Record.Exception(() => builder.WithMaxLength(0));

            Assert.Null(exception);
        }

        [Fact]
        public void WithTTL_Negative_Throws()
        {
            var builder = NewBuilder();

            Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithTTL(TimeSpan.FromSeconds(-1)));
        }

        [Fact]
        public void WithTTL_AtInt32MillisecondOverflow_Throws()
        {
            var builder = NewBuilder();

            Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithTTL(TimeSpan.FromMilliseconds(int.MaxValue)));
        }

        [Fact]
        public void WithTTL_ValidValue_SetsArgument()
        {
            var options = new QueueOptions();
            var builder = new QueueBuilder(options);

            builder.WithTTL(TimeSpan.FromSeconds(30));

            Assert.Equal(30000, options.Arguments["x-message-ttl"]);
        }

        [Fact]
        public void WithExpiration_Negative_Throws()
        {
            var builder = NewBuilder();

            Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithExpiration(TimeSpan.FromSeconds(-1)));
        }

        [Fact]
        public void WithMaxPriority_Zero_Throws()
        {
            var builder = NewBuilder();

            Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithMaxPriority(0));
        }

        [Fact]
        public void WithMaxPriority_AboveMax_Throws()
        {
            var builder = NewBuilder();

            Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithMaxPriority(256));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(255)]
        public void WithMaxPriority_WithinRange_DoesNotThrow(int priority)
        {
            var builder = NewBuilder();

            var exception = Record.Exception(() => builder.WithMaxPriority(priority));

            Assert.Null(exception);
        }
    }
}
