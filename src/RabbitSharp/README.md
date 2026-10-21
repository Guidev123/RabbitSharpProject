# RabbitSharp

A highly customizable RabbitMQ Pub/Sub library for .NET 8+ applications with built-in retry mechanisms and dead letter queue support.

## Features

- ✅ Full Pub/Sub pattern implementation
- ✅ Targets .NET 8, .NET 9 and .NET 10
- ✅ Highly customizable configuration
- ✅ Built-in retry mechanisms with exponential backoff, one retry queue per delay
- ✅ Dead letter queue support, including a dead-letter exchange on the main queue
- ✅ Fail-loud defaults: a full queue rejects the publish instead of discarding messages
- ✅ Network recovery and connection resilience, with a fail-fast window while the broker is down
- ✅ Configurable heartbeat intervals
- ✅ Multiple host support for high availability
- ✅ Flexible exchange and routing configuration
- ✅ Convention-based naming with customizable overrides
- ✅ Configurable JSON serialization, case-insensitive by default
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

`AddRabbitSharp` must be called exactly once per `IServiceCollection`: a second call throws instead of
validating its configuration and then silently keeping the first one.

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

With Messaging Configuration (serialization and the convention-based exchange type):
```csharp
builder.Services.AddRabbitSharp(options =>
{
    // broker options
}, busResilienceOptions: null, busMessagingOptions: messaging =>
{
    // Both convention-based overloads use this type, and they must agree with each other.
    messaging.DefaultExchangeType = ExchangeTypeEnum.Fanout;

    // Case-insensitive by default; set a naming policy when producers are not .NET.
    messaging.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;

    // A payload whose properties did not bind arrives with an empty CorrelationId. By default such
    // a message is dead-lettered instead of being handed to the handler as an all-defaults instance.
    // True is the right default precisely because you cannot count on every producer populating
    // CorrelationId correctly - an empty one is the most reliable signal that a payload never bound.
    // Set this to false only for a message type that legitimately never carries one.
    messaging.RejectMessagesWithEmptyCorrelationId = true;
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

```csharp
using var scope = serviceScopeFactory.CreateScope();
var bus = scope.ServiceProvider.GetRequiredService<IBus>();
var eventSimple = new EventSimple(Guid.NewGuid());
await bus.PublishAsync(eventSimple);
```

## Publishing Messages

Basic Publishing
The simplest way to publish a message.
When using basic publishing, RabbitSharp automatically creates:

- Exchange: Topic type, named after your message class in lower case (e.g. `EventSimple` -> `eventsimple`)
- Routing key: the message class name (e.g. `EventSimple`) - a concrete key, not a pattern
- Queue binding: `MessageName.#` (e.g. `EventSimple.#`), so a subscriber also receives the sub-keys a
  conventional producer publishes, such as `EventSimple.created`

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
            queue.WithTTL(TimeSpan.FromDays(7));
        })
        .WithRoutingKey("event.simple")
        .Build();
        
    bus.SubscribeAsync<EventSimple>(options, HandleEventAsync);
}
```

`WithTTL` and `WithExpiration` take anything under ~24.8 days: AMQP carries both as 32-bit
milliseconds, and a longer span throws `ArgumentOutOfRangeException` at startup rather than being
rejected by the broker later.

On a topic exchange, `WithRoutingKey` is the concrete key used when publishing; use
`WithBindingPattern` when the queue should be bound with a wildcard such as `event.#`. With only a
routing key set, the queue is bound with exactly that key.

### Knowing when a subscription dies

A deleted queue, an expired queue, a policy change or a cluster failover makes the broker cancel the
consumer. Nothing throws, so subscribe to `IBus.SubscriptionInterrupted` to report unhealthy or to
re-subscribe:

```csharp
bus.SubscriptionInterrupted += args =>
{
    logger.LogError("Subscription on {Queue} stopped: {Reason}", args.QueueName, args.Reason);
    return Task.CompletedTask;
};
```

## Environment-Specific Presets
RabbitSharp provides pre-configured setups for different scenarios:

```csharp
// For development environments: smaller queue, and queues that expire when unused
var devOptions = new BusInfrastructureBuilder("my-queue")
    .ForDevelopment()
    .Build();

// For high-throughput scenarios: a higher queue length limit
var highThroughputOptions = new BusInfrastructureBuilder("my-queue")
    .ForHighThroughput()
    .Build();

// For ordered processing: a single active consumer at a time
var orderedOptions = new BusInfrastructureBuilder("my-queue")
    .ForOrderedProcessing()
    .Build();
```

`ForOrderedProcessing()` guarantees one active consumer, which preserves order on first delivery. It
does not preserve order across failures: a message that fails goes to the retry queue and comes back
behind messages published after it. Set `MaxDeliveryRetryAttempts = 0` if strict ordering matters more
than retrying.

## Exchange Types
RabbitSharp supports the following exchange types:

- Topic: Pattern-based routing (default for convention-based publishing)
- Direct: Exact routing key matching
- Fanout: Broadcast to all bound queues

The convention-based overloads (`PublishAsync<T>(message)` and `SubscribeAsync<T>(queueName, handler)`)
both use `BusMessagingOptions.DefaultExchangeType`. Declaring the same exchange with two different
types is rejected by the broker with 406 PRECONDITION_FAILED, so change that single setting rather
than mixing a Fanout subscription with a convention-based publish.

## Failure Handling

Per subscription, RabbitSharp declares:

- the main queue, with a dead-letter exchange pointing at the dead-letter queue, so overflow, TTL
  expiry and rejections end up there instead of disappearing
- one retry queue per distinct backoff delay (`{queue}.retry.1000ms`, `{queue}.retry.2000ms`, ...),
  each with a fixed `x-message-ttl` that dead-letters back into the main queue. One queue per delay is
  what keeps a long wait from holding up the shorter ones behind it, since RabbitMQ only evaluates a
  per-message TTL at the head of the queue
- the dead-letter queue

What happens to a failing delivery:

| Situation | Outcome |
| --- | --- |
| Handler throws, attempts left | Published to the retry queue for this attempt, then redelivered |
| Handler throws, attempts exhausted | Published to the dead-letter queue with `x-final-retry-count` |
| Body is `null`, malformed, or its properties did not bind | Dead-lettered immediately - another attempt cannot make it readable |
| Handler succeeded but the ack failed | Logged as such and left alone: the broker redelivers it, so handlers should tolerate a redelivery. It is not retried or dead-lettered, which would duplicate work that already succeeded |
| Retry/dead-letter publish itself fails | Nacked without requeue, so the main queue dead-letter exchange takes it instead of looping |

## Upgrading an existing deployment

The default queue arguments changed, and RabbitMQ rejects a redeclaration whose arguments differ from
the existing queue with `406 PRECONDITION_FAILED`. A queue declared by an earlier version carries
`x-overflow: drop-head` and no dead-letter exchange, so the first `SubscribeAsync` after the upgrade
fails against it. Pick one per queue:

- delete the old queue (drain it first) and let this version declare it, or
- keep the old arguments explicitly, with
  `WithMainQueue(q => q.WithOverflowBehavior(OverFlowStrategyEnum.DropHead))` and
  `WithMainQueue(q => q.WithDeadLetterExchange(...))` - an explicitly configured dead-letter exchange
  is never overridden

The single `{queue}.retry` queue is no longer declared; the per-delay queues
(`{queue}.retry.1000ms`, ...) take its place. Drain the old one and delete it - messages still sitting
in it dead-letter back into the main queue as usual, but nothing publishes to it any more.

## Best Practices

- RabbitSharp comes with defaults that fail loudly: every queue it declares overflows with
  `reject-publish`, so a full queue surfaces as a `PublishException` instead of quietly dropping the
  oldest message while the publisher is told the publish succeeded
- Retry queues and dead letter queues are declared for you per subscription, and the main queue gets a
  dead-letter exchange so broker-side discards are captured too
- A publish issued while the connection is down waits up to `AutomaticRecoveryWaitTimeout` (30 s by
  default). Lower it, or lower `RecoveryFailFastWindow`, for request-path publishes that must fail fast
- Wrong credentials and a missing virtual host are not retried: they fail immediately instead of
  spending the whole connect backoff budget
