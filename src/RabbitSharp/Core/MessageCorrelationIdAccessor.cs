using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace RabbitSharp.Core
{
    /// <summary>
    /// Reads a message type's <c>CorrelationId</c> by convention - a public <c>Guid CorrelationId</c>
    /// property - instead of through <see cref="Abstractions.IMessage"/>.
    /// </summary>
    /// <remarks>
    /// This is what lets a project that only defines message contracts (DTOs shared between a
    /// producer and its consumers) skip a reference to RabbitSharp, and everything it pulls in
    /// (RabbitMQ.Client, logging abstractions, ...), entirely. <see cref="Abstractions.IMessage"/>
    /// still exists and keeps working for anyone who implements it, but nothing in this library
    /// requires it any more - every generic constraint that used to read <c>where T : IMessage</c> is
    /// now just <c>where T : class</c>, and this class is what reads <c>CorrelationId</c> off the
    /// message by convention in its place.
    /// <para>
    /// The cost of giving up the compile-time guarantee is that a message type missing the property
    /// only fails the first time it is actually published or consumed, rather than at the call site -
    /// <see cref="Get{T}"/> throws a specific, actionable <see cref="InvalidOperationException"/> for
    /// that, and the accessor it builds is compiled once per type and cached, so every later call for
    /// the same type costs a property read, not a reflection lookup.
    /// </para>
    /// </remarks>
    internal static class MessageCorrelationIdAccessor
    {
        private static readonly ConcurrentDictionary<Type, Func<object, Guid>> _accessors = new();

        public static Guid Get<T>(T message) where T : class
        {
            var accessor = _accessors.GetOrAdd(typeof(T), BuildAccessor);
            return accessor(message);
        }

        private static Func<object, Guid> BuildAccessor(Type messageType)
        {
            var property = messageType.GetProperty("CorrelationId", BindingFlags.Public | BindingFlags.Instance);

            if (property is null || property.PropertyType != typeof(Guid) || property.GetMethod is null)
            {
                throw new InvalidOperationException(
                    $"'{messageType.FullName}' cannot be used as a RabbitSharp message: it must expose " +
                    "a public 'Guid CorrelationId { get; }' property. It does not need to implement " +
                    "RabbitSharp.Abstractions.IMessage for this - any type with that property works.");
            }

            var instance = Expression.Parameter(typeof(object), "instance");
            var typedInstance = Expression.Convert(instance, messageType);
            var propertyAccess = Expression.Property(typedInstance, property);
            var lambda = Expression.Lambda<Func<object, Guid>>(propertyAccess, instance);

            return lambda.Compile();
        }
    }
}
