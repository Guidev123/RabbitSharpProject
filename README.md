# RabbitSharp

A highly customizable RabbitMQ Pub/Sub library for .NET 8+ applications with built-in retry mechanisms and dead letter queue support.

## Features

- ✅ Full Pub/Sub pattern implementation
- ✅ Targets .NET 8, .NET 9 and .NET 10
- ✅ Highly customizable configuration
- ✅ Built-in retry mechanisms with exponential backoff - in-process by default, or through a single
  physical retry queue
- ✅ Dead letter queue support, including a dead-letter exchange on the main queue
- ✅ Fail-loud defaults: a full queue rejects the publish instead of discarding messages
- ✅ Network recovery and connection resilience, with a fail-fast window while the broker is down
- ✅ Configurable heartbeat intervals
- ✅ Multiple host support for high availability
- ✅ Flexible exchange and routing configuration
- ✅ Convention-based naming with customizable overrides
- ✅ Configurable JSON serialization, case-insensitive by default
- ✅ Pre-configured setups for different environments
- ✅ No interface required on message contracts - a plain class or record with a `CorrelationId`
  property works, so a shared contracts project never needs to reference RabbitSharp
- ✅ `appsettings.json` support - `AddRabbitSharp(configuration)` binds every options type from
  configuration, with code-based overrides layered on top
- ✅ Multiple virtual hosts (or brokers) from the same application - `AddRabbitSharp(name, ...)`
  registers an additional, independently configured bus resolved through keyed DI

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
  },
  "BusResilienceOptions": {
    "MaxDeliveryRetryAttempts": 3,
    "InitialDeliveryRetryDelay": "00:00:01",
    "MaxDeliveryRetryDelay": "00:00:10"
  },
  "BusMessagingOptions": {
    "DefaultExchangeType": "Topic"
  }
}
```

`BusResilienceOptions` and `BusMessagingOptions` are optional - a missing section just leaves that
options type at its own defaults.

2. Register RabbitSharp in Program.cs

The simplest way is to hand `AddRabbitSharp` your `IConfiguration` directly - it binds
`BrokerOptions`, `BusResilienceOptions` and `BusMessagingOptions` from the sections named after each
type (`BrokerOptions`, `BusResilienceOptions`, `BusMessagingOptions`), the same names used throughout
this README and in the appsettings.json above:

```csharp
builder.Services.AddRabbitSharp(builder.Configuration);
```

`AddRabbitSharp` must be called exactly once per `IServiceCollection`: a second call throws instead of
validating its configuration and then silently keeping the first one.

A section that is missing leaves that options type at its own defaults - and for `BrokerOptions`,
whose `Hosts`/`Username`/`Password` have no sensible default, that still fails validation the same way
an empty lambda would, with the same message. Pass `configureBroker`/`configureResilience`/
`configureMessaging` delegates to layer code-based configuration *on top of* what was bound from
configuration - handy for a secret that should not live in `appsettings.json`, since the delegate runs
after binding and has the last word:

```csharp
builder.Services.AddRabbitSharp(
    builder.Configuration,
    configureBroker: broker =>
    {
        // Overrides whatever BrokerOptions:Password bound from appsettings.json, if anything.
        broker.Password = builder.Configuration["BrokerPasswordSecret"] ?? broker.Password;
    },
    configureResilience: resilience =>
    {
        // Default is RetryStrategy.InMemory - see "Retry Strategies" below.
        resilience.Strategy = RetryStrategy.RetryQueue;
    });
```

A non-default section name works too, e.g. for a shared appsettings.json where `BrokerOptions` is
nested somewhere else:

```csharp
builder.Services.AddRabbitSharp(builder.Configuration, brokerSectionName: "Messaging:Broker");
```

Everything above also works built up entirely in code, with no `IConfiguration` involved at all -
useful for a console app, a test host, or values that do not come from configuration:

```csharp
builder.Services.AddRabbitSharp(options =>
{
    options.Hosts = new[] { "localhost" };
    options.Username = "guest";
    options.Password = "guest";
}, busResilienceOptions =>
{
    busResilienceOptions.InitialDeliveryRetryDelay = TimeSpan.FromSeconds(1);
    busResilienceOptions.MaxDeliveryRetryDelay = TimeSpan.FromSeconds(10);
    busResilienceOptions.MaxDeliveryRetryAttempts = 3;

    // Default is RetryStrategy.InMemory - see "Retry Strategies" below.
    busResilienceOptions.Strategy = RetryStrategy.InMemory;
}, busMessagingOptions: messaging =>
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

`IMessage` is optional - see [Message Requirements](#message-requirements) below. A project that only
defines message contracts (shared between a producer and its consumers) does not need to reference
RabbitSharp at all for this.

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

## Message Requirements

Every `PublishAsync`/`SubscribeAsync` overload only needs the message type to be a reference type
exposing a public `Guid CorrelationId { get; }` property, read by convention at runtime rather than
through an interface:

```csharp
public sealed record YourMessage
{
    public Guid CorrelationId { get; init; }

    // Your message properties here
}
```

`IMessage` still exists and keeps working for anyone who implements it - it is a purely compile-time
convenience (it documents the property and catches a missing one at build time) - but it is not
required. This is what lets a project that only defines message contracts, shared between a producer
and its consumers, skip a reference to RabbitSharp - and everything it pulls in (RabbitMQ.Client,
logging abstractions, ...) - entirely: a plain class or record with that one property is a valid
RabbitSharp message on its own, with no `RabbitSharp.Abstractions` package of any kind to add.

The price of dropping the interface is the compile-time guarantee: a message type missing the
property compiles fine and only fails the first time it is actually published or consumed, with an
`InvalidOperationException` naming the type and the property it is missing. That check is also cached
per type, so it never repeats the reflection lookup once a type has been validated.

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

This fires only for a subscription that will *not* come back on its own, so re-subscribing on it is
safe. A dropped connection is not one of those cases: RabbitMQ.Client's automatic recovery
re-registers the consumer itself, which is logged but raises no event - re-subscribing there would
add a second consumer to the queue on every network blip.

## Multiple Virtual Hosts or Brokers

A RabbitMQ virtual host is pinned to a connection for that connection's whole lifetime at the AMQP
protocol level, so serving more than one from the same application always means more than one
physical connection - there is no way around that. `AddRabbitSharp(name, ...)` registers an
additional, independently configured bus under `name`, so each virtual host (or broker) gets its own
connection without touching the unnamed registration at all:

```csharp
builder.Services.AddRabbitSharp(broker =>
{
    broker.Hosts = ["localhost"];
    broker.VirtualHost = "/";
    broker.Username = "guest";
    broker.Password = "guest";
});

builder.Services.AddRabbitSharp("billing", broker =>
{
    broker.Hosts = ["localhost"];
    broker.VirtualHost = "billing";
    broker.Username = "guest";
    broker.Password = "guest";
});
```

It also works with `IConfiguration`, binding by default from a section named after `name`
(`"billing:BrokerOptions"`, `"billing:BusResilienceOptions"`, `"billing:BusMessagingOptions"`), so
several virtual hosts can each have their own section in the same `appsettings.json`:

```csharp
builder.Services.AddRabbitSharp("billing", builder.Configuration);
```

A named bus is resolved by its name through keyed DI instead of `GetRequiredService<IBus>()`:

```csharp
public sealed class BillingConsumer([FromKeyedServices("billing")] IBus billingBus)
{
}
```

```csharp
var billingBus = serviceProvider.GetRequiredKeyedService<IBus>("billing");
```

Calling `AddRabbitSharp(name, ...)` twice with the same name throws, the same way calling the unnamed
overload twice does; two different names coexist freely, and so does mixing one unnamed call with any
number of named ones. `INamingConventions` and the dead-letter/retry failure-handling service stay
shared across every bus, named or not - both are stateless, so there is nothing to gain from a copy
per bus.

## Environment-Specific Presets
RabbitSharp provides pre-configured setups for different scenarios:

```csharp
// For development environments: a smaller main queue, deleted (with its messages) after an hour
// with no consumer. The retry queue (only declared under RetryStrategy.RetryQueue) never expires -
// nothing consumes from it, so it would be deleted while the subscription is still healthy and
// later retries would stop happening.
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

`ForOrderedProcessing()` guarantees one active consumer, which preserves order on first delivery. With
the default `RetryStrategy.InMemory` it also preserves order across failures, because a failed message
is retried in place rather than moved anywhere. Under `RetryStrategy.RetryQueue` a failed message is
republished and comes back behind messages published after it; set `MaxDeliveryRetryAttempts = 0` if
strict ordering matters more than retrying there.

## Exchange Types
RabbitSharp supports the following exchange types:

- Topic: Pattern-based routing (default for convention-based publishing)
- Direct: Exact routing key matching
- Fanout: Broadcast to all bound queues

The convention-based overloads (`PublishAsync<T>(message)` and `SubscribeAsync<T>(queueName, handler)`)
both use `BusMessagingOptions.DefaultExchangeType`. Declaring the same exchange with two different
types is rejected by the broker with 406 PRECONDITION_FAILED, so change that single setting rather
than mixing a Fanout subscription with a convention-based publish.

## Retry Strategies

`BusResilienceOptions.Strategy` picks how a failed delivery is retried. Both share the same
`MaxDeliveryRetryAttempts`/`InitialDeliveryRetryDelay`/`MaxDeliveryRetryDelay` knobs, but behave very
differently operationally:

| | `RetryStrategy.InMemory` (default) | `RetryStrategy.RetryQueue` |
| --- | --- | --- |
| Broker topology per subscription | Main queue + dead-letter queue. No retry queue at all | Main queue + dead-letter queue + **one** retry queue |
| Backoff | Exponential, up to `MaxDeliveryRetryDelay` | Constant, equal to `InitialDeliveryRetryDelay` |
| While waiting to retry | The delivery stays unacknowledged in the handler, holding a prefetch slot | The delivery is acked; a republish with a per-message TTL sits in the retry queue |
| Across a process restart / subscription cancellation | Retry progress is lost - a redelivered message retries from attempt 0 | Retry progress survives - the retry queue still holds it |
| Ordering under `ForOrderedProcessing()` | Preserved across failures too | Only preserved for the first delivery attempt |

In-memory retry holds the delivery open for the whole backoff window, so `Validate()` rejects a
configuration whose worst-case total backoff (every attempt at its capped delay, summed) exceeds
`MaxInMemoryRetryWindow` (15 minutes by default) - comfortably under RabbitMQ's own `consumer_timeout`
(30 minutes by default), which would otherwise close the channel out from under a delivery that has
been unacknowledged for too long. Pick `RetryStrategy.RetryQueue` instead for backoffs that need to run
longer than that, or raise `MaxInMemoryRetryWindow` alongside the broker's `consumer_timeout`.

```csharp
busResilienceOptions.Strategy = RetryStrategy.RetryQueue;
```

## Failure Handling

Per subscription, RabbitSharp declares:

- the main queue, with a dead-letter exchange pointing at the dead-letter queue, so overflow, TTL
  expiry and rejections end up there instead of disappearing
- the retry queue (`{queue}.retry` by default) - only under `RetryStrategy.RetryQueue`, and only when
  `MaxDeliveryRetryAttempts > 0`. It carries no fixed TTL of its own; the delay is set per message
  instead (see below), so changing `InitialDeliveryRetryDelay` between deployments never redeclares
  this queue with a different argument
- the dead-letter queue

What happens to a failing delivery:

| Situation | Outcome |
| --- | --- |
| Handler throws, attempts left (`InMemory`) | The delivery stays unacknowledged while this attempt's backoff delay elapses, then the handler runs again on the same delivery |
| Handler throws, attempts left (`RetryQueue`) | Acked, and published to the retry queue with a per-message TTL equal to `InitialDeliveryRetryDelay`; redelivered once it dead-letters back to the main queue |
| Handler throws, attempts exhausted | Published to the dead-letter queue with `x-final-retry-count` |
| Body is `null`, malformed, or its properties did not bind | Dead-lettered immediately with `x-final-retry-count: 0` - another attempt cannot make it readable |
| Handler succeeded but the ack failed | Logged as such and left alone: the broker redelivers it, so handlers should tolerate a redelivery. It is not retried or dead-lettered, which would duplicate work that already succeeded |
| `InMemory`: the subscription is cancelled (or the bus disposed) while a delivery is waiting out its backoff | The delivery is nacked with requeue; it is retried from attempt 0 on its next delivery, since no attempt count was ever published anywhere |
| `RetryQueue`: retry publish fails (retry queue deleted or full) | Dead-lettered instead, so the failure costs the retry and not the message |
| Any republish to the retry or dead-letter queue | Carries every property the producer set - `content-type`, `correlation-id`, `reply-to`, `type`, `app-id`, `priority`, `timestamp`, headers. `delivery-mode` is forced to persistent; `expiration` and `user-id` are dropped (see below) |
| Dead-letter publish also fails (dead-letter queue deleted or at `x-max-length`) | Requeued on the main queue and the subscription is **stopped**, raising `SubscriptionInterrupted` with `FailureHandlingUnavailable`. Nacking it without requeue would dead-letter it into the very queue that just rejected it, and the broker discards a dead-lettered message a `reject-publish` queue refuses - so the message would be destroyed. Drain or raise the limit of the dead-letter queue, then re-subscribe |

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

**`BusResilienceOptions.Strategy` now defaults to `RetryStrategy.InMemory` instead of always using a
retry queue.** This changes the broker topology: no retry queue is declared by default any more. The
per-delay queues an earlier version declared (`{queue}.retry.1000ms`, `{queue}.retry.2000ms`, ...) are
simply no longer published to and drain themselves as their messages dead-letter back to the main
queue in the normal course of things; delete them once drained. If retry progress surviving a restart,
or the delivery not sitting unacknowledged through its backoff, matters more to you than the simpler
topology, set `busResilienceOptions.Strategy = RetryStrategy.RetryQueue` to keep the previous behavior
- it now declares a **single** `{queue}.retry` queue rather than one per delay, with a constant delay
  equal to `InitialDeliveryRetryDelay` instead of the previous exponential backoff (a single queue
  cannot hold a growing delay without a later, shorter wait queuing up behind an earlier, longer one -
  RabbitMQ only evaluates a queue's head for expiry). Drain and delete the old per-delay queues either
  way; this version never declares or reads from them again.

A blank queue name is now rejected by `SubscribeAsync`, `BusInfrastructureBuilder` and
`QueueBuilder.WithName` instead of being accepted. It never worked: AMQP reads a blank name in
`queue.bind` and `basic.consume` as "the last queue declared on this channel", so such a subscription
was silently consuming from one of its own retry queues. If this throws for you on upgrade, that
subscription was already bound to the wrong queue.

`ForDevelopment()` no longer puts `x-expires` on the retry queue, only on the main queue. A retry
queue declared by an earlier version still carries it, and redeclaring it without that argument fails
with `406 PRECONDITION_FAILED` - delete the old retry queue(s) once drained and let this version
declare a fresh one.

A dead-lettered message now carries the producer's `timestamp` instead of the time it was
dead-lettered; the time of death moved to the `x-failed-at` header, which has always been there.
Anything reading a dead-letter queue and treating `timestamp` as "when this failed" should read
`x-failed-at` instead.

Two properties are dropped from the republish on purpose, and carrying them would be worse than
losing them. `expiration` is cleared for a dead-letter publish, since the dead-letter queue has no
dead-letter exchange of its own to catch a message that expired out of it; for a `RetryStrategy.RetryQueue`
republish it instead carries the configured delay on purpose, which is how that strategy avoids a
fixed `x-message-ttl` queue argument that could never change without a `406 PRECONDITION_FAILED` on
redeclare. `user-id` is validated by the broker against the authenticated user of the publishing
connection - and the connection republishing is the consumer's, not the producer's - so a message
from another service would answer `406 PRECONDITION_FAILED - user_id property set to ... but
authenticated user was ...` and close the subscription's channel.

## Best Practices

- RabbitSharp comes with defaults that fail loudly: every queue it declares overflows with
  `reject-publish`, so a full queue surfaces as a `PublishException` instead of quietly dropping the
  oldest message while the publisher is told the publish succeeded
- Dead letter queues are declared for you per subscription, and the main queue gets a dead-letter
  exchange so broker-side discards are captured too. A retry queue is also declared, but only under
  `RetryStrategy.RetryQueue` - the default `RetryStrategy.InMemory` declares none
- A publish issued while the connection is down waits up to `AutomaticRecoveryWaitTimeout` (30 s by
  default). Lower it, or lower `RecoveryFailFastWindow`, for request-path publishes that must fail fast
- Wrong credentials and a missing virtual host are not retried: they fail immediately instead of
  spending the whole connect backoff budget
