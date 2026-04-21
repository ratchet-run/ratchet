package run.ratchet.loadtest.workload;

import run.ratchet.loadtest.metrics.PrometheusRegistryProducer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Random;

@ApplicationScoped
public class LoadTestWorkloadExecutor {

  @Inject PrometheusRegistryProducer prometheusRegistry;

  private static void execute(WorkloadSpec spec, WorkloadType effective) throws Exception {
    if (spec.payload() != null && !spec.payload().isEmpty()) {
      spec.payload().hashCode();
    }

    if (effective == WorkloadType.SLEEP) {
      sleep(effectiveSleepMs(spec));
      return;
    }

    if (effective == WorkloadType.PROBABILISTIC_FAILURE) {
      maybeFail(spec);
      sleep(effectiveSleepMs(spec));
    }
  }

  private static WorkloadType effectiveWorkload(WorkloadSpec spec) {
    if (spec.type() != WorkloadType.MIXED) {
      return spec.type();
    }
    return switch (spec.sequence() % 3) {
      case 0 -> WorkloadType.NOOP;
      case 1 -> WorkloadType.SLEEP;
      default -> WorkloadType.PROBABILISTIC_FAILURE;
    };
  }

  private static void recordAttempt(
      MeterRegistry registry,
      WorkloadType requested,
      WorkloadType effective,
      String outcome,
      Timer.Sample sample) {
    String requestedName = requested.name();
    String effectiveName = effective.name();
    Counter.builder("ratchet.loadtest.jobs.completed")
        .description("Load-test job attempts completed by this node")
        .tag("workload", requestedName)
        .tag("effective_workload", effectiveName)
        .tag("outcome", outcome)
        .register(registry)
        .increment();
    sample.stop(
        Timer.builder("ratchet.loadtest.job.duration")
            .description("Load-test job attempt duration")
            .tag("workload", requestedName)
            .tag("effective_workload", effectiveName)
            .tag("outcome", outcome)
            .register(registry));
  }

  private static void maybeFail(WorkloadSpec spec) {
    double rate = Math.max(0.0, Math.min(1.0, spec.failureRate()));
    if (rate == 0.0) {
      return;
    }
    long seed = 31L * spec.runId().hashCode() + spec.sequence();
    if (new Random(seed).nextDouble() < rate) {
      throw new IllegalStateException(
          "Injected load-test failure for run " + spec.runId() + " job " + spec.sequence());
    }
  }

  private static long effectiveSleepMs(WorkloadSpec spec) {
    long sleepMs = Math.max(0, spec.sleepMs());
    long jitterMs = Math.max(0, spec.sleepJitterMs());
    long spikeMs = Math.max(0, spec.sleepSpikeMs());

    if (jitterMs > 0) {
      sleepMs += deterministicLong(spec, 17, jitterMs + 1);
    }

    double spikeRate = Math.max(0.0, Math.min(1.0, spec.sleepSpikeRate()));
    if (spikeMs > 0 && spikeRate > 0.0 && deterministicDouble(spec, 53) < spikeRate) {
      sleepMs += spikeMs;
    }

    return sleepMs;
  }

  private static long deterministicLong(WorkloadSpec spec, int salt, long bound) {
    return Long.remainderUnsigned(deterministicBits(spec, salt), bound);
  }

  private static double deterministicDouble(WorkloadSpec spec, int salt) {
    return (deterministicBits(spec, salt) >>> 11) * 0x1.0p-53;
  }

  private static long deterministicBits(WorkloadSpec spec, int salt) {
    long x = 31L * spec.runId().hashCode() + spec.sequence() + salt;
    x ^= x >>> 33;
    x *= 0xff51afd7ed558ccdL;
    x ^= x >>> 33;
    x *= 0xc4ceb9fe1a85ec53L;
    x ^= x >>> 33;
    return x;
  }

  private static void sleep(long sleepMs) throws InterruptedException {
    if (sleepMs > 0) {
      Thread.sleep(sleepMs);
    }
  }

  public void execute(
      String runId,
      String workload,
      String sequence,
      String sleepMs,
      String sleepJitterMs,
      String sleepSpikeRate,
      String sleepSpikeMs,
      String failureRate,
      String payload)
      throws Exception {
    execute(
        new WorkloadSpec(
            runId,
            WorkloadType.parse(workload),
            Integer.parseInt(sequence),
            Long.parseLong(sleepMs),
            Long.parseLong(sleepJitterMs),
            Double.parseDouble(sleepSpikeRate),
            Long.parseLong(sleepSpikeMs),
            Double.parseDouble(failureRate),
            payload));
  }

  public void execute(WorkloadSpec spec) throws Exception {
    WorkloadType effective = effectiveWorkload(spec);
    MeterRegistry registry = prometheusRegistry.meterRegistry();
    Timer.Sample sample = Timer.start(registry);
    String outcome = "succeeded";
    try {
      execute(spec, effective);
    } catch (Exception e) {
      outcome = "failed";
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw e;
    } finally {
      recordAttempt(registry, spec.type(), effective, outcome, sample);
    }
  }
}
