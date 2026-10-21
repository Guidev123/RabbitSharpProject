using Xunit;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// Waits for a condition instead of sleeping for a fixed amount of time: a fixed delay that is
    /// long enough today starts failing under load or on slower CI, and a delay that is longer than it
    /// needs to be is paid by every run.
    /// </summary>
    internal static class Wait
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(50);

        public static Task UntilAsync(Func<bool> condition, string because, TimeSpan? timeout = null)
        {
            return UntilAsync(() => Task.FromResult(condition()), because, timeout);
        }

        public static async Task UntilAsync(Func<Task<bool>> condition, string because, TimeSpan? timeout = null)
        {
            var effectiveTimeout = timeout ?? DefaultTimeout;
            var deadline = DateTime.UtcNow + effectiveTimeout;

            while (DateTime.UtcNow < deadline)
            {
                if (await condition())
                {
                    return;
                }

                await Task.Delay(PollInterval);
            }

            Assert.Fail($"Timed out after {effectiveTimeout.TotalSeconds:0.#}s waiting until {because}.");
        }

        /// <summary>
        /// Gives a condition that should stay false the chance to become true, so a test asserting
        /// "this never happens" is not just racing the broker.
        /// </summary>
        public static async Task StaysFalseAsync(Func<bool> condition, string because, TimeSpan? window = null)
        {
            var effectiveWindow = window ?? TimeSpan.FromSeconds(2);
            var deadline = DateTime.UtcNow + effectiveWindow;

            while (DateTime.UtcNow < deadline)
            {
                Assert.False(condition(), $"Expected {because} to stay false, but it became true.");
                await Task.Delay(PollInterval);
            }
        }
    }
}
