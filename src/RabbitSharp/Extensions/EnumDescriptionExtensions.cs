using System.Collections.Concurrent;
using System.ComponentModel;
using System.Reflection;

namespace RabbitSharp.Extensions
{
    internal static class EnumDescriptionExtensions
    {
        private static readonly ConcurrentDictionary<Enum, string> _cache = new();

        public static string GetEnumDescription(this Enum value)
        {
            return _cache.GetOrAdd(value, static v =>
            {
                FieldInfo? fi = v.GetType().GetField(v.ToString());
                if (fi is null)
                {
                    return v.ToString();
                }

                var attributes = (DescriptionAttribute[])fi.GetCustomAttributes(typeof(DescriptionAttribute), false);

                return attributes.Length != 0 ? attributes[0].Description : v.ToString();
            });
        }
    }
}
