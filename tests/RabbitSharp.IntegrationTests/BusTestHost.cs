using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitSharp.Abstractions;
using RabbitSharp.DependencyInjection;
using RabbitSharp.Options;

namespace RabbitSharp.IntegrationTests
{
    /// <summary>
    /// Builds an isolated <see cref="IServiceProvider"/>/<see cref="IBus"/> pointed at a test
    /// container, so each test gets its own singleton connection/bus without interfering with
    /// others sharing the same broker.
    /// </summary>
    internal static class BusTestHost
    {
        public static (ServiceProvider Services, IBus Bus) Create(
            RabbitMqContainerFixture fixture,
            string? clientProvidedName = null,
            Action<BrokerOptions>? configureBroker = null,
            Action<BusResilienceOptions>? configureResilience = null)
        {
            var services = new ServiceCollection();
            services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Information));

            services.AddRabbitSharp(b =>
            {
                fixture.ConfigureBroker(b, clientProvidedName);
                b.TryConnectMaxRetries = 3;
                b.NetworkRecoveryIntervalInSeconds = 2;
                configureBroker?.Invoke(b);
            }, configureResilience);

            var provider = services.BuildServiceProvider();
            return (provider, provider.GetRequiredService<IBus>());
        }
    }
}
