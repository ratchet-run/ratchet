/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.loadtest.workload;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Random;

@ApplicationScoped
public class LoadTestWorkloadExecutor {

  @Inject MeterRegistry registry;
  private static volatile int payloadHashSink;

  private static void execute(WorkloadSpec spec, WorkloadType effective) throws Exception {
    if (spec.payload() != null && !spec.payload().isEmpty()) {
      payloadHashSink = spec.payload().hashCode();
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

  private static int parseInt(String name, String raw) {
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be a valid integer: " + raw, e);
    }
  }

  private static long parseLong(String name, String raw) {
    try {
      return Long.parseLong(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be a valid long: " + raw, e);
    }
  }

  private static double parseDouble(String name, String raw) {
    try {
      return Double.parseDouble(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be a valid decimal: " + raw, e);
    }
  }

  /**
   * Executes a deterministic load-test workload from string parameters supplied by the scheduler.
   *
   * @param runId logical load-test run id
   * @param workload workload type name, for example {@code noop}, {@code cpu}, or {@code io}
   * @param sequence integer sequence number within the run
   * @param sleepMs base sleep duration in milliseconds
   * @param sleepJitterMs maximum deterministic sleep jitter in milliseconds
   * @param sleepSpikeRate decimal probability from {@code 0.0} to {@code 1.0} for adding a spike
   * @param sleepSpikeMs additional sleep duration in milliseconds when a spike is selected
   * @param failureRate decimal probability from {@code 0.0} to {@code 1.0} for throwing a synthetic
   *     failure
   * @param payload opaque payload string included in the workload spec
   * @throws IllegalArgumentException if a numeric parameter cannot be parsed
   */
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
            parseInt("sequence", sequence),
            parseLong("sleepMs", sleepMs),
            parseLong("sleepJitterMs", sleepJitterMs),
            parseDouble("sleepSpikeRate", sleepSpikeRate),
            parseLong("sleepSpikeMs", sleepSpikeMs),
            parseDouble("failureRate", failureRate),
            payload));
  }

  public void execute(WorkloadSpec spec) throws Exception {
    WorkloadType effective = effectiveWorkload(spec);
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
