using Microsoft.Extensions.DependencyInjection;
using RabbitSharp.Abstractions;
using RabbitSharp.DependencyInjection;
using RabbitSharp.Options;
using Xunit;

namespace RabbitSharp.UnitTests.DependencyInjection
{
    public class BusConfigurationTests
    {
        private static Action<BrokerOptions> Broker(string host) => options =>
        {
            options.Hosts = new[] { host };
            options.Username = "guest";
            options.Password = "guest";
        };

        [Fact]
        public void AddRabbitSharp_RegistersTheBus()
        {
            var services = new ServiceCollection();

            services.AddRabbitSharp(Broker("localhost"));

            Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IBus));
        }

        [Fact]
        public void AddRabbitSharp_CalledTwice_ThrowsInsteadOfKeepingTheFirstConfiguration()
        {
            var services = new ServiceCollection();
            services.AddRabbitSharp(Broker("first.broker"));

            var ex = Assert.Throws<InvalidOperationException>(
                () => services.AddRabbitSharp(Broker("second.broker")));

            Assert.Contains("already been called", ex.Message);
        }

        [Fact]
        public void AddRabbitSharp_InvalidBrokerOptions_ThrowsAtRegistration()
        {
            var services = new ServiceCollection();

            Assert.Throws<ArgumentException>(() => services.AddRabbitSharp(Broker("localhost:99999")));
        }

        [Fact]
        public void AddRabbitSharp_InvalidMessagingOptions_ThrowsAtRegistration()
        {
            var services = new ServiceCollection();

            Assert.Throws<ArgumentException>(() => services.AddRabbitSharp(
                Broker("localhost"),
                busResilienceOptions: null,
                busMessagingOptions: messaging => messaging.SerializerOptions = null!));
        }
    }
}
