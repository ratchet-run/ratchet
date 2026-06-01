---
sidebar_position: 4
title: Payload and Result Customization
description: Customizing callback payload resolution and job result persistence
---

# Payload and Result Customization

Ratchet persists job work as a target class, method name, JVM method descriptor, static/instance flag, and argument list. The reference implementation derives that invocation from serializable callbacks using ASM.

Use `JobInvocationResolver` when you need to customize how submitted callbacks become persisted job invocations:

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class AppInvocationResolver implements JobInvocationResolver {
    @Override
    public JobInvocation resolve(Serializable callback) {
        return resolve(callback, List.of());
    }

    @Override
    public JobInvocation resolve(Serializable callback, List<Object> runtimeArguments) {
        // Return a stable invocation that can be persisted and executed later.
        return new JobInvocation(
            "com.example.jobs.InvoiceJobs",
            "run",
            "(Ljava/lang/String;)V",
            false,
            runtimeArguments);
    }
}
```

The resolver must be deterministic. A queued job may execute on another node or after a redeploy, so the target class and method descriptor must still exist when the job runs.

## Result Persistence

Job return values are separate from the invocation payload. The default `ResultPersistenceStrategy` stores JSON metadata in the job row and truncates oversized JSON to a marker document.

Override it to change result serialization, redact values, or disable result persistence:

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class RedactingResultPersistence implements ResultPersistenceStrategy {
    @Override
    public SerializedJobResult serialize(UUID jobId, Object result) {
        if (result == null) {
            return SerializedJobResult.empty();
        }
        return new SerializedJobResult("{\"stored\":true}", result.getClass().getName());
    }
}
```

The default size cap is controlled by `RatchetOptions.builder().payload(p -> p.maxResultBytes(...))`. Set it to `0` to disable truncation. If your `RatchetOptions` producer uses `RatchetOptionsFactory.fromEnvironment()`, the same cap is read from `ratchet.jobs.max-result-bytes` / `RATCHET_JOB_RESULT_MAX_BYTES`.

## Related SPIs

`LambdaAnalyzer` is the bytecode-inspection SPI used to reduce submitted lambdas to a stored Class/Method/Args payload. It is not the primary scheduler extension point for submitted job payloads. New integrations should use `JobInvocationResolver` and `ResultPersistenceStrategy`.
