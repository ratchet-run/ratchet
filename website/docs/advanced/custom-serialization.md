---
sidebar_position: 2
title: Custom Serialization
description: Implementing custom serialization strategies for job payloads
---

# Custom Serialization

Ratchet serializes job payloads (the lambda expressions and their captured arguments) into byte arrays for persistence in the job store. The `SerializationStrategy` SPI controls how this serialization and deserialization happens, allowing you to replace the default implementation with one suited to your application's needs.

## SerializationStrategy SPI

The interface defines two operations -- serializing an object to bytes, and reconstructing it from bytes:

```java
package run.ratchet.spi;

public interface SerializationStrategy {

    /**
     * Serializes the given object into a byte array.
     *
     * @param obj the object to serialize; must not be null
     * @return a byte array representing the serialized form
     * @throws IllegalArgumentException if the object cannot be serialized
     */
    byte[] serialize(Object obj);

    /**
     * Deserializes the given byte array into an object of the specified type.
     *
     * @param data the byte array to deserialize; must not be null
     * @param type the target class; must not be null
     * @return the deserialized object
     * @throws IllegalArgumentException if deserialization fails
     */
    <T> T deserialize(byte[] data, Class<T> type);
}
```

Implementations must be **thread-safe** -- multiple job execution threads call `serialize()` and `deserialize()` concurrently.

## Default JDK Serialization

The reference implementation ships with `JdkSerializationStrategy`, which uses Java's standard `ObjectOutputStream` and `ObjectInputStream`. This is the default when no alternative is provided.

### Security Protections

The default strategy applies a strict `ObjectInputFilter` during deserialization. Only classes matching an allowlist are permitted -- all others are rejected. The allowed types include:

- **Ratchet framework types** (`run.ratchet.**`)
- **Primitive arrays** (`[B`, `[I`, `[J`, etc.)
- **Java standard library types** (`String`, `Integer`, `Long`, boxed primitives, etc.)
- **Collections** (`ArrayList`, `HashMap`, `HashSet`, `LinkedList`, immutable collections, etc.)
- **Date/time types** (`java.time.*`)
- **Math types** (`java.math.*`)
- **Specific exception types** (`RuntimeException`, `IOException`, etc.)

Types not on the allowlist trigger a deserialization rejection, preventing gadget-chain attacks. This is a defense-in-depth measure complementing the `ClassPolicy` SPI that validates target classes before execution.

### Limitations

JDK serialization requires that all captured lambda arguments implement `java.io.Serializable`. This works well for primitives, strings, and simple value objects, but becomes a constraint with complex domain objects or third-party types that are not serializable.

## Implementing a Jackson-Based Strategy

For applications already using Jackson, a JSON-based serialization strategy avoids the JDK serialization constraints and produces human-readable payloads:

```java
import run.ratchet.spi.SerializationStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class JacksonSerializationStrategy implements SerializationStrategy {

    private final ObjectMapper mapper;

    public JacksonSerializationStrategy() {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        // Enable type information so deserialize() can reconstruct the correct type
        this.mapper.activateDefaultTyping(
            mapper.getPolymorphicTypeValidator(),
            ObjectMapper.DefaultTyping.NON_FINAL
        );
    }

    @Override
    public byte[] serialize(Object obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to serialize object of type " + obj.getClass().getName(), e);
        }
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> type) {
        try {
            return mapper.readValue(data, type);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to deserialize to " + type.getName(), e);
        }
    }
}
```

### Registering via CDI

The `@Alternative @Priority(Interceptor.Priority.APPLICATION)` annotations ensure this bean replaces the default `JdkSerializationStrategy` during CDI deployment. No additional configuration is needed -- CDI selects the highest-priority alternative automatically.

## Implementing a Protobuf Strategy

For high-throughput systems where payload size and serialization speed matter, Protocol Buffers provide a compact binary format:

```java
import run.ratchet.spi.SerializationStrategy;
import com.google.protobuf.Any;
import com.google.protobuf.Message;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class ProtobufSerializationStrategy implements SerializationStrategy {

    @Override
    public byte[] serialize(Object obj) {
        if (!(obj instanceof Message message)) {
            throw new IllegalArgumentException(
                "Protobuf strategy requires Message types, got: "
                    + obj.getClass().getName());
        }
        return Any.pack(message).toByteArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] data, Class<T> type) {
        try {
            Any any = Any.parseFrom(data);
            if (!Message.class.isAssignableFrom(type)) {
                throw new IllegalArgumentException(
                    "Cannot deserialize to non-Message type: " + type.getName());
            }
            Message defaultInstance = (Message) type
                .getMethod("getDefaultInstance")
                .invoke(null);
            return (T) any.unpack(defaultInstance.getClass());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to deserialize Protobuf to " + type.getName(), e);
        }
    }
}
```

## Payload Constraints

Regardless of which serialization strategy you use, job payloads in Ratchet must follow certain rules:

### Lambda Arguments Must Be Serializable

When you schedule a job, the lambda and its captured arguments are serialized for storage. All captured values must be serializable by whatever strategy is configured:

```java
// Good -- String and int are serializable by all strategies
scheduler.schedule(job -> emailService.send("user@example.com", 3));

// Good -- method reference with no captured state
scheduler.schedule(job -> reportService.generateDailyReport());

// Risky -- EntityManager is not serializable
EntityManager em = ...;
scheduler.schedule(job -> em.find(Order.class, 42)); // Will fail at serialization time
```

### Prefer Value Objects Over Entities

Pass simple value objects or IDs rather than full JPA entities. This avoids serialization issues with lazy-loaded proxies and detached entity state:

```java
// Preferred -- pass the ID, resolve the entity at execution time
long orderId = order.getId();
scheduler.schedule(job -> orderService.process(orderId));

// Avoid -- the entity may have lazy proxies or managed state
scheduler.schedule(job -> orderService.process(order));
```

### Payload Size

Serialized payloads are stored in the database. Keep them small -- pass identifiers and lookup keys rather than large data structures. If a job needs to process a large dataset, store the data externally and pass a reference (S3 key, database ID, etc.).

## Security Considerations

Deserialization is a well-known attack vector. Ratchet provides multiple layers of protection:

1. **SerializationStrategy allowlist** -- The default `JdkSerializationStrategy` uses an `ObjectInputFilter` that rejects any class not on the allowlist.

2. **ClassPolicy SPI** -- Before executing a deserialized job target, the `ClassPolicy` validates that the target class is in an allowed package. The default `PackagePrefixClassPolicy` rejects all classes unless their package is explicitly configured.

3. **Input validation** -- The `JobPayloadInputValidator` checks payload structure before persistence.

When implementing a custom serialization strategy, apply equivalent protections:

```java
@Override
public <T> T deserialize(byte[] data, Class<T> type) {
    // Validate the target type before deserializing
    if (!isAllowedType(type)) {
        throw new SecurityException(
            "Deserialization of type " + type.getName() + " is not permitted");
    }
    // ... perform deserialization
}
```

## Testing Custom Strategies

Test that your strategy correctly round-trips all types your application uses as job arguments:

```java
@Test
void shouldRoundTripJobPayload() {
    JacksonSerializationStrategy strategy = new JacksonSerializationStrategy();

    OrderRequest original = new OrderRequest("SKU-123", 5, BigDecimal.valueOf(29.99));
    byte[] serialized = strategy.serialize(original);
    OrderRequest restored = strategy.deserialize(serialized, OrderRequest.class);

    assertEquals(original.sku(), restored.sku());
    assertEquals(original.quantity(), restored.quantity());
    assertEquals(original.price(), restored.price());
}

@Test
void shouldHandleNullFields() {
    JacksonSerializationStrategy strategy = new JacksonSerializationStrategy();

    OrderRequest original = new OrderRequest("SKU-123", 5, null);
    byte[] serialized = strategy.serialize(original);
    OrderRequest restored = strategy.deserialize(serialized, OrderRequest.class);

    assertNull(restored.price());
}

@Test
void shouldRejectUnknownTypes() {
    JacksonSerializationStrategy strategy = new JacksonSerializationStrategy();

    byte[] garbage = new byte[] { 0x00, 0x01, 0x02 };
    assertThrows(IllegalArgumentException.class,
        () -> strategy.deserialize(garbage, OrderRequest.class));
}
```
