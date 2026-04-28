# Logging Examples

Ratchet emits framework logs through [JBoss Logging](https://github.com/jboss-logging/jboss-logging),
which is a facade that auto-detects an installed backend at runtime. This
directory shows how to wire up the recommended setup for non-WildFly
deployments: **SLF4J as the application logging API + Logback as the
backend + JBoss Logging routing Ratchet's framework logs through it**.

The resulting deployment has a single MDC context map shared by
application code and Ratchet's framework code.

## How the pieces fit

```
┌────────────────────────┐         ┌────────────────────────┐
│ Application code       │         │ Ratchet framework      │
│   org.slf4j.Logger     │         │   org.jboss.logging.   │
│   org.slf4j.MDC        │         │   Logger / MDC         │
└──────────┬─────────────┘         └──────────┬─────────────┘
           │                                  │
           │ slf4j-api 2.x                    │ jboss-logging 3.x
           │                                  │ (detects Logback
           │                                  │  via classpath)
           ▼                                  ▼
       ┌──────────────────────────────────────────────┐
       │         Logback (logback-classic 1.5.x)      │
       │   single thread-local MDC, single JsonEncoder│
       └──────────────────────────────────────────────┘
```

When `ch.qos.logback.classic.Logger` is on the classpath, JBoss
Logging activates its `Slf4jLoggerProvider`. Ratchet's
`org.jboss.logging.MDC.put("jobId", ...)` and your application's
`org.slf4j.MDC.put("requestId", ...)` end up in the **same** MDC
adapter. Both keys appear together in log output.

## Files in this directory

- [`logback.xml`](logback.xml) — drop-in Logback config with built-in
  `JsonEncoder` for structured output. Inline comments explain the
  bridging behavior and container caveats.

## Application setup

### Maven dependencies

```xml
<!-- Application logging facade -->
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-api</artifactId>
  <version>2.0.16</version>
</dependency>

<!-- Logging backend (auto-detected by JBoss Logging) -->
<dependency>
  <groupId>ch.qos.logback</groupId>
  <artifactId>logback-classic</artifactId>
  <version>1.5.18</version>
</dependency>
```

You do **not** need a bridge artifact. JBoss Logging detects Logback
by class presence and routes through SLF4J automatically. Override
detection with `-Dorg.jboss.logging.provider=slf4j` on the JVM if the
default ordering does not match your deployment.

### Application code

Set up the application logger and use SLF4J's MDC for any
correlation IDs your jobs should inherit:

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class OrderJob implements SerializableRunnable {

  private static final Logger log = LoggerFactory.getLogger(OrderJob.class);

  private final long orderId;
  private final String requestId;

  public OrderJob(long orderId, String requestId) {
    this.orderId = orderId;
    this.requestId = requestId;
  }

  @Override
  public void run() {
    MDC.put("requestId", requestId);
    try {
      log.info("Processing order {}", orderId);
      // ... business logic
    } finally {
      MDC.remove("requestId");
    }
  }
}
```

When this job runs, every log line emitted via `log.info(...)`
includes both Ratchet's MDC (`jobId`, `node`, `jobCreator`) and your
application's MDC (`requestId`). With the JSON encoder in
`logback.xml`, the output looks like:

```json
{
  "timestamp": 1746029834521,
  "level": "INFO",
  "loggerName": "com.example.OrderJob",
  "mdc": {
    "jobId": "42",
    "node": "node-a",
    "jobCreator": "alice@example.com",
    "requestId": "req-7c3f"
  },
  "message": "Processing order 12345"
}
```

A correlation ID set on the request thread (e.g. by a Servlet filter
or JAX-RS interceptor) is *not* automatically propagated into the
job thread — MDC is thread-local. Pass the ID through the job
payload (as the `requestId` constructor parameter above) and re-bind
it on the worker thread inside the job.

## Container notes

| Container | Recommended approach |
|---|---|
| OpenLiberty | Drop `logback.xml` into the deployment classpath. Liberty's default `java.util.logging` will be replaced. |
| Payara (Web Profile, embedded mode) | Same — `logback.xml` on classpath; JBoss Logging detects Logback. |
| Plain JVM / `java -jar` | Same. |
| WildFly | Not the recommended setup. WildFly ships JBoss LogManager natively; substituting Logback requires `META-INF/jboss-deployment-structure.xml` classloader exclusions. Configure JSON output via the `logging` subsystem instead. |
| GlassFish 8 | Similar to WildFly. Prefer GlassFish's native log manager. |
| Quarkus | Quarkus uses JBoss LogManager via `quarkus-logging-jboss-logmanager`; configure JSON via `quarkus.log.console.json=true` instead of using Logback. |

## Verifying the setup

1. Drop `logback.xml` into `src/main/resources/` of an application that
   uses Ratchet.
2. Add the SLF4J + Logback dependencies above.
3. Enqueue a job and observe the output. Each log line emitted from
   inside the job (whether by Ratchet or by your application) should
   be a single JSON object with `mdc.jobId` populated.

If `mdc.jobId` is missing from output, JBoss Logging is not routing
through Logback. Common causes:

- Logback is not on the classpath (check `mvn dependency:tree`).
- An application server's classloader is shadowing Logback with the
  container's logging stack — see the container note above.
- A different provider was forced via system property or service-loader
  configuration. Set `-Dorg.jboss.logging.provider=slf4j` to verify.
