using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// Captures what the library logged, so a test can assert on the diagnostic itself - which log
    /// an operator gets is the whole point of some of these behaviours, and nothing else observes it.
    /// </summary>
    internal sealed class TestLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<LogRecord> _records = new();

        public IReadOnlyList<LogRecord> Records => _records.ToArray();

        public ILogger CreateLogger(string categoryName) => new TestLogger(categoryName, _records);

        public void Dispose()
        {
        }

        /// <param name="EventName">
        /// The name the <c>LoggerMessage</c> source generator puts on the event: the logging method's
        /// own name, which is more durable to assert on than its numeric id.
        /// </param>
        internal sealed record LogRecord(
            string Category,
            string? EventName,
            int EventId,
            LogLevel Level,
            string Message,
            Exception? Exception);

        private sealed class TestLogger : ILogger
        {
            private readonly string _category;
            private readonly ConcurrentQueue<LogRecord> _records;

            public TestLogger(string category, ConcurrentQueue<LogRecord> records)
            {
                _category = category;
                _records = records;
            }

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                _records.Enqueue(new LogRecord(
                    _category,
                    eventId.Name,
                    eventId.Id,
                    logLevel,
                    formatter(state, exception),
                    exception));
            }
        }
    }
}
