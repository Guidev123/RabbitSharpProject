using Microsoft.Extensions.Configuration;
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
        /// Registers the bus, its connection manager and the default naming conventions, with
        /// <see cref="BrokerOptions"/>, <see cref="BusResilienceOptions"/> and
        /// <see cref="BusMessagingOptions"/> bound from <paramref name="configuration"/> instead of
        /// built up entirely in code.
        /// </summary>
        /// <param name="configuration">
        /// Bound onto each options type by <see cref="ConfigurationBinder.Bind(IConfiguration, object)"/>
        /// before the matching <paramref name="configureBroker"/>/<paramref name="configureResilience"/>/
        /// <paramref name="configureMessaging"/> delegate runs - so code-based configuration always has
        /// the last word, for a value (a secret from a vault, say) that should not live in
        /// <c>appsettings.json</c>.
        /// </param>
        /// <param name="configureBroker">
        /// Optional. Applied after <paramref name="configuration"/> is bound, on top of it - not instead
        /// of it, the way the other <c>AddRabbitSharp</c> overload's <c>brokerOptions</c> works.
        /// </param>
        /// <param name="configureResilience">Optional; see <paramref name="configureBroker"/>.</param>
        /// <param name="configureMessaging">Optional; see <paramref name="configureBroker"/>.</param>
        /// <param name="brokerSectionName">
        /// The configuration section <see cref="BrokerOptions"/> is bound from. Default is
        /// <c>"BrokerOptions"</c> (<c>nameof(BrokerOptions)</c>), so <c>appsettings.json</c> can use the
        /// same name shown throughout this library's own documentation.
        /// </param>
        /// <param name="resilienceSectionName">
        /// The configuration section <see cref="BusResilienceOptions"/> is bound from. Default is
        /// <c>"BusResilienceOptions"</c>.
        /// </param>
        /// <param name="messagingSectionName">
        /// The configuration section <see cref="BusMessagingOptions"/> is bound from. Default is
        /// <c>"BusMessagingOptions"</c>.
        /// </param>
        /// <remarks>
        /// A missing section binds nothing, leaving that options type at its own defaults (and, for
        /// <see cref="BrokerOptions"/>, still fails <see cref="BrokerOptions.Validate"/> the same way an
        /// empty <c>Action&lt;BrokerOptions&gt;</c> would, since <see cref="BrokerOptions.Hosts"/>,
        /// <see cref="BrokerOptions.Username"/> and <see cref="BrokerOptions.Password"/> have none to
        /// fall back on). <see cref="BusMessagingOptions.SerializerOptions"/> binds like any other
        /// nested object - its own scalar properties (e.g. <c>PropertyNameCaseInsensitive</c>) can be
        /// set from configuration, but a collection such as <c>Converters</c> cannot and needs
        /// <paramref name="configureMessaging"/> instead.
        /// </remarks>
        /// <exception cref="ArgumentNullException">
        /// Thrown if <paramref name="configuration"/> is <see langword="null"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when called more than once on the same <see cref="IServiceCollection"/> - see the
        /// other <c>AddRabbitSharp</c> overload.
        /// </exception>
        public static IServiceCollection AddRabbitSharp(
            this IServiceCollection services,
            IConfiguration configuration,
            Action<BrokerOptions>? configureBroker = null,
            Action<BusResilienceOptions>? configureResilience = null,
            Action<BusMessagingOptions>? configureMessaging = null,
            string brokerSectionName = nameof(BrokerOptions),
            string resilienceSectionName = nameof(BusResilienceOptions),
            string messagingSectionName = nameof(BusMessagingOptions))
        {
            ArgumentNullException.ThrowIfNull(configuration);

            return services.AddRabbitSharp(
                broker =>
                {
                    configuration.GetSection(brokerSectionName).Bind(broker);
                    configureBroker?.Invoke(broker);
                },
                resilience =>
                {
                    configuration.GetSection(resilienceSectionName).Bind(resilience);
                    configureResilience?.Invoke(resilience);
                },
                messaging =>
                {
                    configuration.GetSection(messagingSectionName).Bind(messaging);
                    configureMessaging?.Invoke(messaging);
                });
        }

        /// <summary>
        /// Registers an additional, independently configured bus under <paramref name="name"/>, with
        /// <see cref="BrokerOptions"/>, <see cref="BusResilienceOptions"/> and
        /// <see cref="BusMessagingOptions"/> bound from <paramref name="configuration"/>.
        /// </summary>
        /// <param name="name">
        /// The key this bus is registered and resolved under - see the other <c>AddRabbitSharp(string, ...)</c>
        /// overload.
        /// </param>
        /// <param name="configuration">See the unnamed <c>AddRabbitSharp(IConfiguration, ...)</c> overload.</param>
        /// <param name="configureBroker">See the unnamed <c>AddRabbitSharp(IConfiguration, ...)</c> overload.</param>
        /// <param name="configureResilience">See the unnamed <c>AddRabbitSharp(IConfiguration, ...)</c> overload.</param>
        /// <param name="configureMessaging">See the unnamed <c>AddRabbitSharp(IConfiguration, ...)</c> overload.</param>
        /// <param name="brokerSectionName">
        /// The configuration section <see cref="BrokerOptions"/> is bound from. Default is
        /// <c>"{name}:BrokerOptions"</c>, so several named buses can each have their own section in the
        /// same <c>appsettings.json</c> without colliding on the plain <c>"BrokerOptions"</c> name the
        /// unnamed overload uses.
        /// </param>
        /// <param name="resilienceSectionName">
        /// The configuration section <see cref="BusResilienceOptions"/> is bound from. Default is
        /// <c>"{name}:BusResilienceOptions"</c>.
        /// </param>
        /// <param name="messagingSectionName">
        /// The configuration section <see cref="BusMessagingOptions"/> is bound from. Default is
        /// <c>"{name}:BusMessagingOptions"</c>.
        /// </param>
        /// <exception cref="ArgumentException">
        /// Thrown if <paramref name="name"/> is <see langword="null"/>, empty or whitespace.
        /// </exception>
        /// <exception cref="ArgumentNullException">
        /// Thrown if <paramref name="configuration"/> is <see langword="null"/>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when called more than once with the same <paramref name="name"/> on the same
        /// <see cref="IServiceCollection"/> - see the other <c>AddRabbitSharp(string, ...)</c> overload.
        /// </exception>
        public static IServiceCollection AddRabbitSharp(
            this IServiceCollection services,
            string name,
            IConfiguration configuration,
            Action<BrokerOptions>? configureBroker = null,
            Action<BusResilienceOptions>? configureResilience = null,
            Action<BusMessagingOptions>? configureMessaging = null,
            string? brokerSectionName = null,
            string? resilienceSectionName = null,
            string? messagingSectionName = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            ArgumentNullException.ThrowIfNull(configuration);

            return services.AddRabbitSharp(
                name,
                broker =>
                {
                    configuration.GetSection(brokerSectionName ?? $"{name}:{nameof(BrokerOptions)}").Bind(broker);
                    configureBroker?.Invoke(broker);
                },
                resilience =>
                {
                    configuration.GetSection(resilienceSectionName ?? $"{name}:{nameof(BusResilienceOptions)}").Bind(resilience);
                    configureResilience?.Invoke(resilience);
                },
                messaging =>
                {
                    configuration.GetSection(messagingSectionName ?? $"{name}:{nameof(BusMessagingOptions)}").Bind(messaging);
                    configureMessaging?.Invoke(messaging);
                });
        }

        /// <summary>
        /// Registers the bus, its connection manager and the default naming conventions.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when called more than once on the same <see cref="IServiceCollection"/>. Every
        /// registration here is a <c>TryAdd</c>, so a second call would validate its configuration and
        /// then silently discard it, leaving the application pointed at the first broker - a duplicated
        /// line in <c>Program.cs</c>, or two packages that each register the bus, would be invisible.
        /// Call the <c>AddRabbitSharp(string name, ...)</c> overload instead to register an additional,
        /// independently configured bus - for a second RabbitMQ virtual host, say.
        /// </exception>
        public static IServiceCollection AddRabbitSharp(
            this IServiceCollection services,
            Action<BrokerOptions> brokerOptions,
            Action<BusResilienceOptions>? busResilienceOptions = null,
            Action<BusMessagingOptions>? busMessagingOptions = null)
        {
            return AddRabbitSharpCore(services, null, brokerOptions, busResilienceOptions, busMessagingOptions);
        }

        /// <summary>
        /// Registers an additional, independently configured bus - its own <see cref="IConnectionManager"/>
        /// and <see cref="IBus"/>, each with its own connection - resolved by <paramref name="name"/>
        /// through keyed DI (<see cref="ServiceProviderKeyedServiceExtensions.GetRequiredKeyedService{T}"/>,
        /// or a <c>[FromKeyedServices(name)]</c> constructor parameter) instead of
        /// <see cref="ServiceProviderServiceExtensions.GetRequiredService{T}"/>.
        /// </summary>
        /// <remarks>
        /// This is the way to connect to more than one RabbitMQ virtual host, or more than one broker,
        /// from the same application: a virtual host is pinned to a connection for that connection's
        /// whole lifetime at the AMQP protocol level, so there is no way to serve several of them over
        /// one <see cref="IConnectionManager"/>. <see cref="IBusFailureHandlingService"/> and
        /// <see cref="INamingConventions"/> stay shared across every bus, named or not - both are
        /// stateless, so there is nothing to gain from a copy per bus.
        /// <para>
        /// The unnamed <c>AddRabbitSharp</c> overload is unaffected by this one existing: it keeps
        /// registering <see cref="IBus"/> and <see cref="IConnectionManager"/> unkeyed, exactly as
        /// before, so an application with a single bus needs no changes at all to keep working.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentException">
        /// Thrown if <paramref name="name"/> is <see langword="null"/>, empty or whitespace.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when called more than once with the same <paramref name="name"/> on the same
        /// <see cref="IServiceCollection"/>.
        /// </exception>
        public static IServiceCollection AddRabbitSharp(
            this IServiceCollection services,
            string name,
            Action<BrokerOptions> brokerOptions,
            Action<BusResilienceOptions>? busResilienceOptions = null,
            Action<BusMessagingOptions>? busMessagingOptions = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);

            return AddRabbitSharpCore(services, name, brokerOptions, busResilienceOptions, busMessagingOptions);
        }

        private static IServiceCollection AddRabbitSharpCore(
            IServiceCollection services,
            object? serviceKey,
            Action<BrokerOptions> brokerOptions,
            Action<BusResilienceOptions>? busResilienceOptions,
            Action<BusMessagingOptions>? busMessagingOptions)
        {
            if (IsAlreadyRegistered(services, serviceKey))
            {
                throw new InvalidOperationException(BuildAlreadyRegisteredMessage(serviceKey));
            }

            AddRegistrationMarker(services, serviceKey);

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

            if (serviceKey is null)
            {
                services.TryAddSingleton<IConnectionManager>(sp =>
                    new ConnectionManager(brokerConfig, sp.GetRequiredService<ILogger<ConnectionManager>>()));

                services.TryAddSingleton<IBus>(sp => new Bus(
                    sp.GetRequiredService<IBusFailureHandlingService>(),
                    sp.GetRequiredService<IConnectionManager>(),
                    sp.GetRequiredService<ILogger<Bus>>(),
                    sp.GetRequiredService<INamingConventions>(),
                    busResilienceOptions,
                    busMessagingOptions));
            }
            else
            {
                services.TryAddKeyedSingleton<IConnectionManager>(serviceKey, (sp, _) =>
                    new ConnectionManager(brokerConfig, sp.GetRequiredService<ILogger<ConnectionManager>>()));

                services.TryAddKeyedSingleton<IBus>(serviceKey, (sp, _) => new Bus(
                    sp.GetRequiredService<IBusFailureHandlingService>(),
                    sp.GetRequiredKeyedService<IConnectionManager>(serviceKey),
                    sp.GetRequiredService<ILogger<Bus>>(),
                    sp.GetRequiredService<INamingConventions>(),
                    busResilienceOptions,
                    busMessagingOptions));
            }

            return services;
        }

        private static bool IsAlreadyRegistered(IServiceCollection services, object? serviceKey)
        {
            return services.Any(descriptor =>
                descriptor.ServiceType == typeof(RabbitSharpRegistrationMarker) &&
                descriptor.IsKeyedService == (serviceKey is not null) &&
                (serviceKey is null || Equals(descriptor.ServiceKey, serviceKey)));
        }

        private static void AddRegistrationMarker(IServiceCollection services, object? serviceKey)
        {
            if (serviceKey is null)
            {
                services.AddSingleton<RabbitSharpRegistrationMarker>();
            }
            else
            {
                services.AddKeyedSingleton<RabbitSharpRegistrationMarker>(serviceKey);
            }
        }

        private static string BuildAlreadyRegisteredMessage(object? serviceKey)
        {
            return serviceKey is null
                ? "AddRabbitSharp has already been called on this IServiceCollection. Calling it twice " +
                  "would keep the first configuration and discard this one, so configure a single call " +
                  "instead - or call the AddRabbitSharp(string name, ...) overload to register an " +
                  "additional, independently configured bus."
                : $"AddRabbitSharp(\"{serviceKey}\", ...) has already been called on this IServiceCollection. " +
                  "Calling it twice with the same name would keep the first configuration and discard this " +
                  "one, so use a different name for another bus.";
        }

        /// <summary>
        /// Registered only so a second <see cref="AddRabbitSharp"/> call - unkeyed, or with the same
        /// name - can be detected.
        /// </summary>
        private sealed class RabbitSharpRegistrationMarker
        {
        }
    }
}
