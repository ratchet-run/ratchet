---
sidebar_position: 5
title: Custom Logging
description: Per-job structured logging with the JobLogger SPI
---

# Custom Logging

Ratchet provides a `JobLogger` SPI that gives each running job its own isolated logger. Log messages are routed through both a logging backend and the internal event system, enabling real-time log streaming and persistent log storage.

## JobLogger SPI

The interface defines five log-level methods:

```java
package run.ratchet.spi;

@Incubating
public interface JobLogger {

    /** Informational messages: job progress, milestones. */
    void info(String message);

    /** Diagnostic detail useful during development. */
    void debug(String message);

    /** Potentially problematic situations that deserve attention. */
    void warn(String message);

    /** Significant failures requiring immediate attention. */
    void error(String message);

    /** Fine-grained execution path tracing. */
    void trace(String message);
}
```

Each job execution receives its own `JobLogger` instance, bound to that job's ID. This ensures log isolation -- messages from concurrent jobs do not interleave or lose context.

## Default JBoss Logging Implementation

The reference implementation provides `JBossLoggingJobLogger`, which bridges job logs to JBoss Logging (which auto-detects the runtime backend — JBoss LogManager, SLF4J, Log4j 2, or JDK JUL) and simultaneously publishes them as internal events for persistence.

> **Note:** As of 0.2.0, `JBossLoggingJobLogger` is reserved for future per-job logger wiring and is not currently instantiated by `JobTask`. The `JobLogger` SPI is wired via a no-op implementation by default, and applications that need custom job logging can plug in their own implementation following the pattern below.

```java
public class JBossLoggingJobLogger implements JobLogger {

    private final long jobId;
    private final InternalEventPublisher eventPublisher;

    public JBossLoggingJobLogger(long jobId, InternalEventPublisher eventPublisher) {
        this.jobId = jobId;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void info(String message) {
        log.infof("[Job %d] %s", jobId, message);
        publishLogLine(LogLevel.INFO, message);
    }

    @Override
    public void debug(String message) {
        log.debugf("[Job %d] %s", jobId, message);
        publishLogLine(LogLevel.DEBUG, message);
    }

    @Override
    public void warn(String message) {
        log.warnf("[Job %d] %s", jobId, message);
        publishLogLine(LogLevel.WARN, message);
    }

    @Override
    public void error(String message) {
        log.errorf("[Job %d] %s", jobId, message);
        publishLogLine(LogLevel.ERROR, message);
    }

    @Override
    public void trace(String message) {
        log.tracef("[Job %d] %s", jobId, message);
        publishLogLine(LogLevel.TRACE, message);
    }

    private void publishLogLine(LogLevel level, String message) {
        if (eventPublisher != null) {
            Map<String, Object> mdcSnapshot = MDC.getMap() == null
                ? new HashMap<>()
                : new HashMap<>(MDC.getMap());
            eventPublisher.publish(
                new JobLogLine(jobId, Instant.now(), level, message, mdcSnapshot));
        }
    }
}
```

The dual routing means:

1. **Backend log output** -- Log messages appear in the container's standard log output (console, log files), prefixed with `[Job <id>]`. The actual backend depends on what JBoss Logging detects at startup: JBoss LogManager on WildFly, the JUL fallback otherwise.
2. **Event publishing** -- Log lines are published as `JobLogLine` events through the `InternalEventPublisher`, which routes them to the `JobLogStore` for database persistence and to any registered event listeners for real-time streaming.

### Level Mapping

| JobLogger Method | JBoss Logging Level | Backend Mapping |
|------------------|---------------------|-----------------|
| `info()` | `INFO` | INFO across all backends |
| `debug()` | `DEBUG` | DEBUG / FINE under JUL |
| `warn()` | `WARN` | WARN / WARNING under JUL |
| `error()` | `ERROR` | ERROR / SEVERE under JUL |
| `trace()` | `TRACE` | TRACE / FINEST under JUL |

## Using JobLogger in Job Tasks

The `JobLogger` is available through the `JobContext` passed to your job task:

```java
scheduler.newJob()
    .task(ctx -> {
        JobLogger logger = ctx.getLogger();
        logger.info("Starting order processing");

        List<Order> orders = orderRepository.findPending();
        logger.debug("Found " + orders.size() + " pending orders");

        for (Order order : orders) {
            try {
                orderService.process(order);
                logger.trace("Processed order " + order.getId());
            } catch (Exception e) {
                logger.warn("Failed to process order " + order.getId()
                    + ": " + e.getMessage());
            }
        }

        logger.info("Completed order processing");
    })
    .submit();
```

## Implementing a Custom JobLogger

### SLF4J Integration

Replace JUL with SLF4J for applications using Logback, Log4j2, or other SLF4J-compatible backends:

```java
import run.ratchet.spi.JobLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class Slf4jJobLogger implements JobLogger {

    private static final Logger log = LoggerFactory.getLogger("ratchet.job");

    private final long jobId;

    public Slf4jJobLogger(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public void info(String message) {
        withMdc(() -> log.info(message));
    }

    @Override
    public void debug(String message) {
        withMdc(() -> log.debug(message));
    }

    @Override
    public void warn(String message) {
        withMdc(() -> log.warn(message));
    }

    @Override
    public void error(String message) {
        withMdc(() -> log.error(message));
    }

    @Override
    public void trace(String message) {
        withMdc(() -> log.trace(message));
    }

    private void withMdc(Runnable action) {
        MDC.put("jobId", String.valueOf(jobId));
        try {
            action.run();
        } finally {
            MDC.remove("jobId");
        }
    }
}
```

With a Logback pattern like:

```xml
<pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} job=%X{jobId} - %msg%n</pattern>
```

This produces structured log output:

```
14:23:45.123 [ratchet-worker-3] INFO  ratchet.job job=42 - Starting order processing
14:23:45.234 [ratchet-worker-3] DEBUG ratchet.job job=42 - Found 15 pending orders
```

### Structured JSON Logger

For log aggregation systems (ELK, Datadog Logs, CloudWatch Logs) that consume JSON:

```java
import run.ratchet.spi.JobLogger;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import java.time.Instant;
import java.util.logging.Logger;

public class JsonJobLogger implements JobLogger {

    private static final Logger log = Logger.getLogger("ratchet.job.json");

    private final long jobId;
    private final String jobType;
    private final String nodeName;

    public JsonJobLogger(long jobId, String jobType, String nodeName) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.nodeName = nodeName;
    }

    @Override
    public void info(String message) {
        emit("INFO", message);
    }

    @Override
    public void debug(String message) {
        emit("DEBUG", message);
    }

    @Override
    public void warn(String message) {
        emit("WARN", message);
    }

    @Override
    public void error(String message) {
        emit("ERROR", message);
    }

    @Override
    public void trace(String message) {
        emit("TRACE", message);
    }

    private void emit(String level, String message) {
        JsonObjectBuilder builder = Json.createObjectBuilder()
            .add("timestamp", Instant.now().toString())
            .add("level", level)
            .add("jobId", jobId)
            .add("jobType", jobType)
            .add("node", nodeName)
            .add("message", message);

        log.info(builder.build().toString());
    }
}
```

### Database-Only Logger

If you only need log persistence without console output:

```java
import run.ratchet.spi.JobLogger;
import run.ratchet.ri.core.InternalEventPublisher;
import run.ratchet.ri.core.JobLogLine;
import run.ratchet.store.entity.JobLogEntity.LogLevel;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class SilentJobLogger implements JobLogger {

    private final long jobId;
    private final InternalEventPublisher eventPublisher;

    public SilentJobLogger(long jobId, InternalEventPublisher eventPublisher) {
        this.jobId = jobId;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void info(String message) {
        publish(LogLevel.INFO, message);
    }

    @Override
    public void debug(String message) {
        publish(LogLevel.DEBUG, message);
    }

    @Override
    public void warn(String message) {
        publish(LogLevel.WARN, message);
    }

    @Override
    public void error(String message) {
        publish(LogLevel.ERROR, message);
    }

    @Override
    public void trace(String message) {
        publish(LogLevel.TRACE, message);
    }

    private void publish(LogLevel level, String message) {
        if (eventPublisher != null) {
            eventPublisher.publish(
                new JobLogLine(jobId, Instant.now(), level, message, new HashMap<>()));
        }
    }
}
```

## Wiring a Custom JobLogger

The `JobLogger` is not a global CDI bean -- each job execution gets its own instance. As of 0.2.0, the default wiring is a no-op `JobLogger`; the engine does not currently instantiate `JBossLoggingJobLogger` automatically. To plug in a custom logger, replace the no-op binding via your own producer or extension point in `JobMdcContext.bindJobContext`.

### Routing Backend Output

Ratchet's framework code logs through JBoss Logging, which auto-detects the runtime backend:

| Runtime | Backend |
|---|---|
| WildFly 30+ | JBoss LogManager (native) |
| Quarkus | JBoss LogManager (via `quarkus-logging-jboss-logmanager`) |
| Spring Boot / Logback | Logback (via SLF4J detection) |
| Standalone JDK | JDK `java.util.logging` (fallback) |

No bridge or extra dependency is required for the framework's own logs. To render the MDC keys (`jobId`, `node`, `jobCreator`) in your output, add `%X{jobId} %X{node} %X{jobCreator}` to your formatter pattern (e.g. in `standalone.xml`, `quarkus.log.console.format`, or `logback.xml`).

## Log Persistence

Job log lines published through the `InternalEventPublisher` are persisted via the `JobLogStore` SPI (part of the store module). This allows you to query historical job logs:

```sql
-- Find recent error logs for a specific job
SELECT level, message, created_at
FROM ratchet_job_log
WHERE job_id = 42
  AND level = 'ERROR'
ORDER BY created_at DESC;
```

The `LogPurgeTimer` in the RI automatically cleans up old log entries based on a configurable retention period, preventing unbounded log table growth.

## Best Practices

**Use appropriate log levels.** Reserve `error()` for actual failures that need investigation. Use `warn()` for recoverable problems. Use `info()` for significant milestones (job started, completed, key steps). Use `debug()` and `trace()` for diagnostic detail that is normally not visible.

**Keep messages concise.** Log messages are persisted to the database. Avoid logging large objects, stack traces, or binary data through the `JobLogger`. For exceptions, log the message and type rather than the full stack trace.

**Include identifiers in messages.** Since each `JobLogger` is already bound to a job ID, include any additional correlation IDs (order ID, customer ID, batch item index) in the message text to aid debugging.

**Leverage the event system.** The `InternalEventPublisher` dispatches log lines synchronously. If your custom logger does expensive work (network calls, file I/O), consider queuing log entries and flushing asynchronously to avoid blocking job execution.

**Configure logger levels per category.** Ratchet's framework loggers use the fully-qualified class name as the logger category (e.g. `run.ratchet.ri.core.JobTask`). Configure levels through your backend of choice. Examples:

WildFly (`standalone.xml`):
```xml
<logger category="run.ratchet">
    <level name="INFO"/>
</logger>
```

Logback (`logback.xml`):
```xml
<logger name="run.ratchet" level="INFO"/>
```

Quarkus (`application.properties`):
```properties
quarkus.log.category."run.ratchet".level=INFO
```
