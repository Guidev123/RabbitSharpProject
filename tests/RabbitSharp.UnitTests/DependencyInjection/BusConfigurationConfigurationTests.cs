using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitSharp.Abstractions;
using RabbitSharp.DependencyInjection;
using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.DependencyInjection
{
    /// <summary>
    /// The <c>AddRabbitSharp(IConfiguration, ...)</c> overload: <see cref="BrokerOptions"/>,
    /// <see cref="BusResilienceOptions"/> and <see cref="BusMessagingOptions"/> bound from
    /// configuration sections named after each type, with any code-based delegate applied on top.
    /// </summary>
    public class BusConfigurationConfigurationTests
    {
        private static IConfiguration BuildConfiguration(Dictionary<string, string?> data) =>
            new ConfigurationBuilder().AddInMemoryCollection(data).Build();

        private static IConfiguration ValidBrokerConfiguration(string host = "localhost") => BuildConfiguration(new()
        {
            ["BrokerOptions:Hosts:0"] = host,
            ["BrokerOptions:Username"] = "guest",
            ["BrokerOptions:Password"] = "guest"
        });

        [Fact]
        public void AddRabbitSharp_BindsBrokerOptionsFromConfiguration_AndRegistersTheBus()
        {
            var services = new ServiceCollection();

            services.AddRabbitSharp(ValidBrokerConfiguration());

            Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IBus));
        }

        [Fact]
        public void AddRabbitSharp_WithNoBrokerSection_FailsValidationJustLikeAnEmptyLambdaWould()
        {
            var services = new ServiceCollection();
            var configuration = BuildConfiguration(new());

            var ex = Assert.Throws<ArgumentException>(() => services.AddRabbitSharp(configuration));

            Assert.Equal("Hosts", ex.ParamName);
        }

        [Fact]
        public void AddRabbitSharp_ConfigureBrokerDelegate_OverridesWhatConfigurationBound()
        {
            var services = new ServiceCollection();
            var configuration = ValidBrokerConfiguration("from-config");

            services.AddRabbitSharp(configuration, configureBroker: broker => broker.Hosts = new[] { "from-code" });

            Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IBus));
        }

        [Fact]
        public void AddRabbitSharp_BindsResilienceOptionsFromConfiguration()
        {
            var services = new ServiceCollection();
            var configuration = BuildConfiguration(new()
            {
                ["BrokerOptions:Hosts:0"] = "localhost",
                ["BrokerOptions:Username"] = "guest",
                ["BrokerOptions:Password"] = "guest",
                ["BusResilienceOptions:MaxDeliveryRetryAttempts"] = "-1"
            });

            var ex = Assert.Throws<ArgumentException>(() => services.AddRabbitSharp(configuration));

            Assert.Equal("MaxDeliveryRetryAttempts", ex.ParamName);
        }

        [Fact]
        public void AddRabbitSharp_CustomSectionNames_BindFromThoseInstead()
        {
            var services = new ServiceCollection();
            var configuration = BuildConfiguration(new()
            {
                ["Rabbit:Hosts:0"] = "localhost",
                ["Rabbit:Username"] = "guest",
                ["Rabbit:Password"] = "guest"
            });

            services.AddRabbitSharp(configuration, brokerSectionName: "Rabbit");

            Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IBus));
        }

        [Fact]
        public void AddRabbitSharp_WithNullConfiguration_Throws()
        {
            var services = new ServiceCollection();

            Assert.Throws<ArgumentNullException>(() => services.AddRabbitSharp((IConfiguration)null!));
        }

        [Fact]
        public void AddRabbitSharp_CalledTwice_ThrowsEvenWhenTheSecondCallUsesConfiguration()
        {
            var services = new ServiceCollection();
            services.AddRabbitSharp(ValidBrokerConfiguration());

            Assert.Throws<InvalidOperationException>(() => services.AddRabbitSharp(ValidBrokerConfiguration()));
        }
    }
}
