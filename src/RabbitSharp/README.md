# RabbitSharp

A high-performance, highly customizable RabbitMQ Pub/Sub library for .NET 6+ applications with built-in retry mechanisms and dead letter queue support.

## Features

- ✅ Full Pub/Sub pattern implementation
- ✅ Compatible with .NET 6+
- ✅ Highly customizable configuration
- ✅ Built-in retry mechanisms with exponential backoff
- ✅ Dead letter queue support
- ✅ Network recovery and connection resilience
- ✅ Configurable heartbeat intervals
- ✅ Multiple host support for high availability
- ✅ Flexible exchange and routing configuration
- ✅ Convention-based naming with customizable overrides
- ✅ Pre-configured setups for different environments
 
## Installation

```bash
dotnet add package RabbitSharp
```

## Quick Start

1. Configure your appsettings.json

```json
{
  "BrokerOptions": {
    "TryConnectMaxRetries": 5,
    "NetworkRecoveryIntervalInSeconds": 10,
    "HeartbeatIntervalSeconds": 60,
    "Hosts": [ "localhost" ],
    "Username": "guest",
    "Password": "guest",
    "VirtualHost": "/"
  }
}
```

2. Register RabbitSharp in Program.cs
```csharp
var brokerOptions = builder.Configuration.GetSection(nameof(BrokerOptions)).Get<BrokerOptions>() ?? new BrokerOptions();
builder.Services.AddRabbitSharp(options =>
{
    options.Username = brokerOptions.Username;
    options.Password = brokerOptions.Password;
    options.VirtualHost = brokerOptions.VirtualHost;
    options.Hosts = brokerOptions.Hosts;
});
```
With Resilience Configuration:
```csharp
var brokerOptions = builder.Configuration.GetSection(nameof(BrokerOptions)).Get<BrokerOptions>() ?? new BrokerOptions();
builder.Services.AddRabbitSharp(options =>
{
    options.Username = brokerOptions.Username;
    options.Password = brokerOptions.Password;
    options.VirtualHost = brokerOptions.VirtualHost;
    options.Hosts = brokerOptions.Hosts;
}, busResilienceOptions =>
{
    busResilienceOptions.InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1);
    busResilienceOptions.MaxDeliveryRetryDelay = TimeSpan.FromSeconds(10);
    busResilienceOptions.MaxDeliveryRetryAttempts = 3;
});
```

3. Create your message class

```csharp
public class EventSimple : IMessage
{
    public EventSimple(Guid correlationId)
    {
        CorrelationId = correlationId;
    }
    
    public EventSimple()
    {
        CorrelationId = Guid.NewGuid();
    }
    
    public Guid CorrelationId { get; init; }
}
```

4. Publish messages

```
using var scope = serviceScopeFactory.CreateScope();
var bus = scope.ServiceProvider.GetRequiredService<IBus>();
var eventSimple = new EventSimple(Guid.NewGuid());
await bus.PublishAsync(eventSimple);
```

## Publishing Messages

Basic Publishing
The simplest way to publish a message.
When using basic publishing, RabbitSharp automatically creates:

- Exchange: Topic type with name based on your message class name (e.g., "EventSimple")
- Routing Key: MessageName.# pattern (e.g., "EventSimple.#")

```csharp
using var scope = serviceScopeFactory.CreateScope();
var bus = scope.ServiceProvider.GetRequiredService<IBus>();
var eventSimple = new EventSimple(Guid.NewGuid());
await bus.PublishAsync(eventSimple);
```

Advanced Publishing with Custom Options
For more control over exchange configuration and routing:

```csharp
using var scope = serviceScopeFactory.CreateScope();
var bus = scope.ServiceProvider.GetRequiredService<IBus>();

var options = new PublisherOptionsBuilder()
    .WithExchange(exchange =>
    {
        exchange.WithName("publisher.event-simple");
        exchange.WithType(ExchangeTypeEnum.Direct);
        exchange.AsDurable();
        exchange.WithArgument("alternate-exchange", "unrouted.exchange");
    })
    .WithRoutingKey("event.simple")
    .Build();

var eventSimple = new EventSimple(Guid.NewGuid());
await bus.PublishAsync(options, eventSimple);
```

Message Requirements
```csharp
public sealed record YourMessage : IMessage
{
    public Guid CorrelationId { get; init; }
    
    // Your message properties here
}
```

5. Subscribing to Messages
Basic Subscription (Convention-Based)
The simplest way to consume messages. RabbitSharp will automatically connect to a Topic exchange named after your message class:

```csharp
private void SetSubscribers()
{
    bus.SubscribeAsync<EventSimple>("subscriber.event-simple", HandleEventAsync);
}
```

Advanced Subscription with Custom Configuration
For full control over queue and exchange configuration:

```csharp
private void SetSubscribers()
{
    var options = new BusInfrastructureBuilder("subscriber.event-simple")
        .WithExchange(exchange =>
        {
            exchange.WithName("publisher.event-simple");
            exchange.WithType(ExchangeTypeEnum.Direct);
            exchange.AsDurable();
            exchange.WithArgument("alternate-exchange", "unrouted.exchange");
        })
        .WithMainQueue(queue =>
        {
            queue.AsDurable();
            queue.WithMaxLength(10000);
            queue.WithMaxPriority(10);
            queue.WithOverflowBehavior(OverFlowStrategyEnum.RejectPublish);
            queue.WithTTL(TimeSpan.FromDays(30));
        })
        .WithRoutingKey("event.simple")
        .Build();
        
    bus.SubscribeAsync<EventSimple>(options, HandleEventAsync);
}
```

## Environment-Specific Presets
RabbitSharp provides pre-configured setups for different scenarios:

```csharp
// For development environments
var devOptions = new BusInfrastructureBuilder("my-queue")
    .ForDevelopment()
    .Build();

// For high-throughput scenarios
var highThroughputOptions = new BusInfrastructureBuilder("my-queue")
    .ForHighThroughput()
    .Build();
```

## Exchange Types
RabbitSharp supports the following exchange types:

- Topic: Pattern-based routing (default for convention-based publishing)
- Direct: Exact routing key matching
- Fanout: Broadcast to all bound queues

## Best Practices

- RabbitSharp comes with sensible defaults following RabbitMQ best practices
- Even without manual configuration, retry queues and dead letter queues are automatically set up
