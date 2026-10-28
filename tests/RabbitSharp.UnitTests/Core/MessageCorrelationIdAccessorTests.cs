using RabbitSharp.Abstractions;
using RabbitSharp.Core;
using Xunit;

namespace RabbitSharp.UnitTests.Core
{
    public class MessageCorrelationIdAccessorTests
    {
        private sealed record ImplementsIMessage(Guid CorrelationId) : IMessage;

        // No RabbitSharp reference of any kind - a plain record with the property by convention,
        // standing in for a type defined in a project that only holds message contracts.
        private sealed record PlainPoco(Guid CorrelationId, string CustomerName);

        private sealed class MissingTheProperty
        {
            public string? Name { get; set; }
        }

        private sealed class WrongPropertyType
        {
            public string CorrelationId { get; set; } = string.Empty;
        }

        private sealed class WriteOnlyCorrelationId
        {
            public Guid CorrelationId { set { } }
        }

        [Fact]
        public void Get_TypeImplementingIMessage_ReadsTheProperty()
        {
            var id = Guid.NewGuid();

            Assert.Equal(id, MessageCorrelationIdAccessor.Get(new ImplementsIMessage(id)));
        }

        [Fact]
        public void Get_PlainTypeWithoutIMessage_ReadsThePropertyByConvention()
        {
            var id = Guid.NewGuid();

            Assert.Equal(id, MessageCorrelationIdAccessor.Get(new PlainPoco(id, "Grace")));
        }

        [Fact]
        public void Get_TypeMissingTheProperty_ThrowsNamingTheType()
        {
            var exception = Assert.Throws<InvalidOperationException>(
                () => MessageCorrelationIdAccessor.Get(new MissingTheProperty()));

            Assert.Contains(typeof(MissingTheProperty).FullName!, exception.Message);
            Assert.Contains("CorrelationId", exception.Message);
        }

        [Fact]
        public void Get_TypeWithWrongPropertyType_Throws()
        {
            Assert.Throws<InvalidOperationException>(
                () => MessageCorrelationIdAccessor.Get(new WrongPropertyType()));
        }

        [Fact]
        public void Get_TypeWithWriteOnlyCorrelationId_Throws()
        {
            Assert.Throws<InvalidOperationException>(
                () => MessageCorrelationIdAccessor.Get(new WriteOnlyCorrelationId()));
        }

        [Fact]
        public void Get_SameTypeCalledTwice_ReturnsEachInstancesOwnValue()
        {
            // Not just that it works twice - that the cached accessor built for the first instance
            // is not somehow bound to that instance's value.
            var first = Guid.NewGuid();
            var second = Guid.NewGuid();

            Assert.Equal(first, MessageCorrelationIdAccessor.Get(new PlainPoco(first, "A")));
            Assert.Equal(second, MessageCorrelationIdAccessor.Get(new PlainPoco(second, "B")));
        }
    }
}
