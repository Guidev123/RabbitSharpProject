using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitSharp.Abstractions;
using RabbitSharp.Core;
using RabbitSharp.DependencyInjection;
using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.DependencyInjection
{
    /// <summary>
    /// The <c>AddRabbitSharp(string name, ...)</c> overloads: an additional, independently configured
    /// bus - one per RabbitMQ virtual host, say - registered through keyed DI instead of the unkeyed
    /// <see cref="IBus"/>/<see cref="IConnectionManager"/> the unnamed overloads use.
    /// </summary>
    public class BusConfigurationNamedTests
    {
        private static Action<BrokerOptions> Broker(string host) => options =>
        {
            options.Hosts = new[] { host };
            options.Username = "guest";
            options.Password = "guest";
        };

        private static IConfiguration BuildConfiguration(Dictionary<string, string?> data) =>
            new ConfigurationBuilder().AddInMemoryCollection(data).Build();

        private static bool HasKeyedRegistration<TService>(IServiceCollection services, object key) =>
            services.Any(d => d.ServiceType == typeof(TService) && d.IsKeyedService && Equals(d.ServiceKey, key));

        private static bool HasUnkeyedRegistration<TService>(IServiceCollection services) =>
            services.Any(d => d.ServiceType == typeof(TService) && !d.IsKeyedService);

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void AddRabbitSharp_NamedLambdaOverload_WithBlankName_Throws(string? name)
        {
            var services = new ServiceCollection();

            Assert.ThrowsAny<ArgumentException>(() => services.AddRabbitSharp(name!, Broker("localhost")));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void AddRabbitSharp_NamedConfigurationOverload_WithBlankName_Throws(string? name)
        {
            var services = new ServiceCollection();
            var configuration = BuildConfiguration(new());

            Assert.ThrowsAny<ArgumentException>(() => services.AddRabbitSharp(name!, configuration));
        }

        [Fact]
        public void AddRabbitSharp_Named_RegistersAKeyedBusAndConnectionManager()
        {
            var services = new ServiceCollection();

            services.AddRabbitSharp("orders-vhost", Broker("localhost"));

            Assert.True(HasKeyedRegistration<IBus>(services, "orders-vhost"));
            Assert.True(HasKeyedRegistration<IConnectionManager>(services, "orders-vhost"));
        }

        [Fact]
        public void AddRabbitSharp_Named_DoesNotRegisterAnUnkeyedBus()
        {
            var services = new ServiceCollection();

            services.AddRabbitSharp("orders-vhost", Broker("localhost"));

            Assert.False(HasUnkeyedRegistration<IBus>(services));
        }

        [Fact]
        public void AddRabbitSharp_TwoDifferentNames_EachGetsItsOwnKeyedBusAndConnectionManager()
        {
            var services = new ServiceCollection();

            services.AddRabbitSharp("orders-vhost", Broker("orders.rabbitmq"));
            services.AddRabbitSharp("billing-vhost", Broker("billing.rabbitmq"));

            Assert.True(HasKeyedRegistration<IBus>(services, "orders-vhost"));
            Assert.True(HasKeyedRegistration<IBus>(services, "billing-vhost"));
            Assert.True(HasKeyedRegistration<IConnectionManager>(services, "orders-vhost"));
            Assert.True(HasKeyedRegistration<IConnectionManager>(services, "billing-vhost"));
        }

        [Fact]
        public void AddRabbitSharp_NamedAndUnnamed_CoexistOnTheSameServiceCollection()
        {
            var services = new ServiceCollection();

            services.AddRabbitSharp(Broker("default.rabbitmq"));
            services.AddRabbitSharp("secondary-vhost", Broker("secondary.rabbitmq"));

            Assert.True(HasUnkeyedRegistration<IBus>(services));
            Assert.True(HasKeyedRegistration<IBus>(services, "secondary-vhost"));
        }

        [Fact]
        public void AddRabbitSharp_NamedBuses_ShareTheSameUnkeyedFailureHandlingServiceAndNamingConventions()
        {
            var services = new ServiceCollection();

            services.AddRabbitSharp("orders-vhost", Broker("orders.rabbitmq"));
            services.AddRabbitSharp("billing-vhost", Broker("billing.rabbitmq"));

            Assert.Single(services, d => d.ServiceType == typeof(INamingConventions));
        }

        [Fact]
        public void AddRabbitSharp_SameNameCalledTwice_ThrowsNamingItInTheMessage()
        {
            var services = new ServiceCollection();
            services.AddRabbitSharp("orders-vhost", Broker("first.rabbitmq"));

            var ex = Assert.Throws<InvalidOperationException>(
                () => services.AddRabbitSharp("orders-vhost", Broker("second.rabbitmq")));

            Assert.Contains("orders-vhost", ex.Message);
            Assert.Contains("already been called", ex.Message);
        }

        [Fact]
        public void AddRabbitSharp_UnnamedCalledTwice_StillThrowsEvenAfterANamedRegistration()
        {
            var services = new ServiceCollection();
            services.AddRabbitSharp("orders-vhost", Broker("first.rabbitmq"));
            services.AddRabbitSharp(Broker("default.rabbitmq"));

            Assert.Throws<InvalidOperationException>(() => services.AddRabbitSharp(Broker("another.rabbitmq")));
        }

        [Fact]
        public void AddRabbitSharp_DifferentNames_DoNotThrow()
        {
            var services = new ServiceCollection();
            services.AddRabbitSharp("orders-vhost", Broker("first.rabbitmq"));

            var exception = Record.Exception(
                () => services.AddRabbitSharp("billing-vhost", Broker("second.rabbitmq")));

            Assert.Null(exception);
        }

        [Fact]
        public void AddRabbitSharp_Named_InvalidBrokerOptions_ThrowsAtRegistration()
        {
            var services = new ServiceCollection();

            Assert.Throws<ArgumentException>(
                () => services.AddRabbitSharp("orders-vhost", Broker("localhost:99999")));
        }

        [Fact]
        public void AddRabbitSharp_NamedWithConfiguration_BindsFromANamePrefixedSectionByDefault()
        {
            var services = new ServiceCollection();
            var configuration = BuildConfiguration(new()
            {
                ["orders-vhost:BrokerOptions:Hosts:0"] = "localhost",
                ["orders-vhost:BrokerOptions:Username"] = "guest",
                ["orders-vhost:BrokerOptions:Password"] = "guest"
            });

            services.AddRabbitSharp("orders-vhost", configuration);

            Assert.True(HasKeyedRegistration<IBus>(services, "orders-vhost"));
        }

        [Fact]
        public void AddRabbitSharp_NamedWithConfiguration_MissingSection_FailsValidation()
        {
            var services = new ServiceCollection();
            var configuration = BuildConfiguration(new());

            var ex = Assert.Throws<ArgumentException>(() => services.AddRabbitSharp("orders-vhost", configuration));

            Assert.Equal("Hosts", ex.ParamName);
        }

        [Fact]
        public void AddRabbitSharp_NamedWithConfiguration_CustomSectionNameOverridesTheDefault()
        {
            var services = new ServiceCollection();
            var configuration = BuildConfiguration(new()
            {
                ["Broker:Hosts:0"] = "localhost",
                ["Broker:Username"] = "guest",
                ["Broker:Password"] = "guest"
            });

            services.AddRabbitSharp("orders-vhost", configuration, brokerSectionName: "Broker");

            Assert.True(HasKeyedRegistration<IBus>(services, "orders-vhost"));
        }

        [Fact]
        public void AddRabbitSharp_NamedWithConfiguration_WithNullConfiguration_Throws()
        {
            var services = new ServiceCollection();

            Assert.Throws<ArgumentNullException>(
                () => services.AddRabbitSharp("orders-vhost", (IConfiguration)null!));
        }
    }
}
