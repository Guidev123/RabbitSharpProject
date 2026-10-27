using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using RabbitSharp.Abstractions;
using RabbitSharp.Core;
using RabbitSharp.Options;
using System.Linq;

namespace RabbitSharp.DependencyInjection
{
    public static class BusConfiguration
    {
        /// <summary>
        /// Registers the bus, its connection manager and the default naming conventions.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when called more than once on the same <see cref="IServiceCollection"/>. Every
        /// registration here is a <c>TryAdd</c>, so a second call would validate its configuration and
        /// then silently discard it, leaving the application pointed at the first broker - a duplicated
        /// line in <c>Program.cs</c>, or two packages that each register the bus, would be invisible.
        /// </exception>
        public static IServiceCollection AddRabbitSharp(
            this IServiceCollection services,
            Action<BrokerOptions> brokerOptions,
            Action<BusResilienceOptions>? busResilienceOptions = null,
            Action<BusMessagingOptions>? busMessagingOptions = null)
        {
            if (services.Any(descriptor => descriptor.ServiceType == typeof(RabbitSharpRegistrationMarker)))
            {
                throw new InvalidOperationException(
                    "AddRabbitSharp has already been called on this IServiceCollection. Calling it twice " +
                    "would keep the first configuration and discard this one, so configure a single call " +
                    "instead - or register your own IBus/IConnectionManager if you need two buses.");
            }

            services.AddSingleton<RabbitSharpRegistrationMarker>();

            var brokerConfig = new BrokerOptions();
            brokerOptions(brokerConfig);

            brokerConfig.Validate();

            if (busResilienceOptions is not null)
            {
                var resilienceConfig = new BusResilienceOptions();
                busResilienceOptions(resilienceConfig);
                resilienceConfig.Validate();
            }

            if (busMessagingOptions is not null)
            {
                var messagingConfig = new BusMessagingOptions();
                busMessagingOptions(messagingConfig);
                messagingConfig.Validate();
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
                    busResilienceOptions,
                    busMessagingOptions);
            });

            return services;
        }

        /// <summary>
        /// Registered only so a second <see cref="AddRabbitSharp"/> call can be detected.
        /// </summary>
        private sealed class RabbitSharpRegistrationMarker
        {
        }
    }
}