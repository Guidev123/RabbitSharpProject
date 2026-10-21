namespace RabbitSharp.Options
{
    /// <summary>
    /// Parses the <c>"host"</c> / <c>"host:port"</c> entries of <see cref="BrokerOptions.Hosts"/>.
    /// </summary>
    internal static class BrokerEndpointParser
    {
        internal const int MinPort = 1;
        internal const int MaxPort = 65535;

        /// <summary>
        /// Parses a <c>"host"</c> or <c>"host:port"</c> entry (and the bracketed IPv6 forms
        /// <c>"[::1]"</c> / <c>"[::1]:5672"</c>), falling back to <paramref name="defaultPort"/> when
        /// no port is present.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when the entry carries a port that is not a number in the range 1-65535, or ends in a
        /// bare colon - which would otherwise travel on as part of the host name and only fail much
        /// later, as a DNS error far from its cause.
        /// </exception>
        internal static (string Host, int Port) Parse(string host, int defaultPort)
        {
            if (host.Length > 0 && host[0] == '[')
            {
                var closeBracket = host.IndexOf(']');
                if (closeBracket > 0)
                {
                    var hostPart = host[1..closeBracket];
                    var rest = host[(closeBracket + 1)..];

                    if (rest.Length == 0)
                    {
                        return (hostPart, defaultPort);
                    }

                    if (rest[0] != ':')
                    {
                        throw InvalidEntry(host, $"unexpected text '{rest}' after the bracketed address");
                    }

                    return (hostPart, ParsePort(host, rest[1..]));
                }
            }

            var firstColon = host.IndexOf(':');
            var lastColon = host.LastIndexOf(':');

            if (firstColon > 0 && firstColon == lastColon)
            {
                return (host[..lastColon], ParsePort(host, host[(lastColon + 1)..]));
            }

            return (host, defaultPort);
        }

        internal static bool IsValidPort(int port)
        {
            return port is >= MinPort and <= MaxPort;
        }

        private static int ParsePort(string host, string portText)
        {
            if (!int.TryParse(portText, out var port) || !IsValidPort(port))
            {
                throw InvalidEntry(host, $"'{portText}' is not a port between {MinPort} and {MaxPort}");
            }

            return port;
        }

        private static ArgumentException InvalidEntry(string host, string reason)
        {
            return new ArgumentException($"Host entry '{host}' is invalid: {reason}.", nameof(host));
        }
    }
}
