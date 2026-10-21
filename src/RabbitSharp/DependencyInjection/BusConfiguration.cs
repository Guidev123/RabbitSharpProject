using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using RabbitSharp.Abstractions;
using RabbitSharp.Core;
using RabbitSharp.Options;

namespace RabbitSharp.DependencyInjection
{
    public static class BusConfiguration
    {
        public static IServiceCollection AddRabbitSharp(
            this IServiceCollection services,
            Action<BrokerOptions> brokerOptions,
            Action<BusResilienceOptions>? busResilienceOptions = null)
        {
            var brokerConfig = new BrokerOptions();
            brokerOptions(brokerConfig);

            brokerConfig.Validate();

            if (busResilienceOptions is not null)
            {
                var resilienceConfig = new BusResilienceOptions();
                busResilienceOptions(resilienceConfig);
                resilienceConfig.Validate();
            }

            services.TryAddSingleton<IBusFailureHandlingService, BusFailureHandlingService>();
            services.TryAddSingleton<INamingConventions, SimpleNamingConventions>();

            services.TryAddSingleton<IConnectionManager>(sp =>
            {
                var logger = sp.GetRequiredService<ILogger<ConnectionManager>>();
                return new ConnectionManager(brokerConfig, logger);
            });

            services.TryAddSingleton<IBus>(serviceProvider =>
            {
                var logger = serviceProvider.GetRequiredService<ILogger<Bus>>();
                var busFailureHandlingService = serviceProvider.GetRequiredService<IBusFailureHandlingService>();
                var namingConventions = serviceProvider.GetRequiredService<INamingConventions>();
                var connectionManager = serviceProvider.GetRequiredService<IConnectionManager>();

                return new Bus(
                    busFailureHandlingService,
                    connectionManager,
                    logger,
                    namingConventions,
                    busResilienceOptions);
            });

            return services;
        }
    }
}