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
package example.ratchet.verification;

import example.denied.DeniedJob;
import jakarta.annotation.PreDestroy;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.Recurring;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.spring.boot.autoconfigure.RegisterRatchetTypes;

/**
 * Executed inside the application process; Testcontainers and assertions stay in the JVM harness.
 */
@org.springframework.context.annotation.Profile("verification")
@org.springframework.context.annotation.Import(RuntimeVerification.class)
@Configuration(proxyBeanMethods = false)
@RegisterRatchetTypes(
    value = {DeniedJob.class, example.library.LibrarySubmitter.class},
    basePackageClasses = NativeVerification.class)
public class NativeVerification {
  public static final Set<String> EXECUTED = ConcurrentHashMap.newKeySet();
  public static final Set<UUID> SUBMITTED = ConcurrentHashMap.newKeySet();
  public static final Map<String, UUID> EXECUTIONS = new ConcurrentHashMap<>();
  static final AtomicInteger RETRIES = new AtomicInteger();
  static final AtomicInteger FAILURES = new AtomicInteger();
  static final AtomicInteger CONSTRUCTED = new AtomicInteger();
  static final AtomicInteger DESTROYED = new AtomicInteger();
  static volatile boolean started;

  @Bean
  Jobs verificationJobs() {
    return new Jobs();
  }

  @Bean
  RecurringBase verificationRecurring(Environment environment) {
    return environment.getProperty("consumer.full-verify", Boolean.class, false)
        ? new RecurringJobs()
        : new RecurringBase();
  }

  @Bean
  @Lazy
  LazyJob verificationLazyJob() {
    return new LazyJob();
  }

  @Bean
  @Scope("prototype")
  PrototypeJob verificationPrototypeJob() {
    return new PrototypeJob();
  }

  @Bean
  SchedulerLifecycleHook verificationHook(Environment environment) {
    return new SchedulerLifecycleHook() {
      @Override
      public void afterStart() {
        started = true;
      }

      @Override
      public void afterStop() {
        if (environment.getProperty("consumer.full-verify", Boolean.class, false))
          System.out.println("RATCHET_GRACEFUL_SHUTDOWN_VERIFIED");
      }
    };
  }

  @Bean
  @Order(-100)
  ApplicationRunner verifyNativeFeatures(
      ApplicationContext context,
      Environment environment,
      ObjectProvider<JobSchedulerService> provider,
      Jobs jobs,
      ClassPolicy policy,
      JobQueryService queries) {
    return args -> {
      if (!environment.getProperty("consumer.full-verify", Boolean.class, false)) return;
      check(started, "lifecycle hook did not run before runners");
      check(CONSTRUCTED.get() == 0, "lazy/prototype jobs were eagerly constructed");
      var scheduler = provider.getObject();
      example.library.LibrarySubmitter.submit(scheduler);
      track(scheduler.enqueueNow(jobs::reference));
      String captured = "capturing";
      track(scheduler.enqueueNow(() -> jobs.record(captured)));
      var record = new RecordArgument("record");
      var pojo = new PojoArgument("pojo");
      track(scheduler.enqueueNow(() -> jobs.recordArgument(record)));
      track(scheduler.enqueueNow(() -> jobs.pojoArgument(pojo)));
      new NestedSubmitter(context).submit(jobs, "nested");
      class LocalSubmitter {
        void submit() {
          track(context.getBean(JobSchedulerService.class).enqueueNow(() -> jobs.record("local")));
        }
      }
      new LocalSubmitter().submit();
      new Runnable() {
        @Override
        public void run() {
          track(
              context
                  .getBean(JobSchedulerService.class)
                  .enqueueNow(() -> jobs.record("anonymous")));
        }
      }.run();
      new InheritedSubmitter(provider).submit(jobs);
      track(context.getBean(JobSchedulerService.class).enqueueNow(jobs::lookupReference));
      var batch =
          scheduler
              .enqueueBatch("native-batch")
              .forEach(List.of("batch-one", "batch-two"), jobs::record)
              .submit();
      var workflow =
          scheduler
              .enqueue(jobs::result)
              .branch(
                  WorkflowCondition.result(Jobs::validResult),
                  jobs::workflow,
                  "native result branch")
              .submit();
      var signal =
          scheduler
              .enqueue(jobs::signal)
              .awaitSignal("native-signal", Duration.ofMinutes(1))
              .submit();
      track(batch);
      track(workflow);
      track(signal);
      check(scheduler.deliverSignal(signal.id(), "delivered") == 1, "signal was not delivered");
      String secret = "native-secret-argument";
      track(
          scheduler
              .enqueue(() -> jobs.encrypted(secret))
              .withBusinessKey("native-encrypted")
              .withEncryptedPayload()
              .submit());
      var retried =
          track(
              scheduler
                  .enqueue(jobs::retryOnce)
                  .withMaxRetries(2)
                  .withBackoff(BackoffPolicy.NONE, Duration.ZERO)
                  .submit());
      var failed =
          scheduler
              .enqueue(jobs::alwaysFail)
              .withBusinessKey("native-dead-letter")
              .withMaxRetries(1)
              .withBackoff(BackoffPolicy.NONE, Duration.ZERO)
              .submit();
      track(scheduler.enqueue(jobs::virtualThread).virtual().submit());
      // Invocation metadata selects managed lazy/prototype targets without retrieving them here.
      var invocations = context.getBean(run.ratchet.spi.InvocationSubmissionService.class);
      track(
          invocations
              .enqueueInvocation(
                  new run.ratchet.spi.JobInvocation(
                      LazyJob.class.getName(), "execute", "()V", false, List.of()))
              .submit());
      track(
          invocations
              .enqueueInvocation(
                  new run.ratchet.spi.JobInvocation(
                      PrototypeJob.class.getName(), "execute", "()V", false, List.of()))
              .submit());
      check(
          !policy.isAllowed(DeniedJob.class.getName()), "native registration changed ClassPolicy");
      boolean rejected = false;
      try {
        scheduler.enqueueNow(DeniedJob::execute);
      } catch (SecurityException failure) {
        check(
            failure
                .getMessage()
                .equals(
                    "Class " + DeniedJob.class.getName() + " is not allowed for job execution."),
            "wrong policy rejection: " + failure);
        rejected = true;
      }
      check(rejected, "submission to denied target was accepted");
      check(
          queries
              .findJobs(JobFilter.builder().targetClass(DeniedJob.class.getName()).build(), 10, 0)
              .items()
              .isEmpty(),
          "denied submission left a stored job");
      Set<String> expected =
          Set.of(
              "reference",
              "capturing",
              "record",
              "pojo",
              "nested",
              "local",
              "anonymous",
              "inherited",
              "lookup-reference",
              "batch-one",
              "batch-two",
              "workflow",
              "signal",
              "encrypted",
              "virtual",
              "lazy",
              "prototype",
              "recurring",
              "retry",
              "automatic-reference",
              "automatic-capturing",
              "automatic-nested",
              "automatic-local",
              "automatic-anonymous",
              "automatic-record",
              "automatic-pojo",
              "library-opt-in");
      await(
          () -> EXECUTED.containsAll(expected) && DESTROYED.get() == 1,
          "Missing executions: " + expected);
      SUBMITTED.addAll(EXECUTIONS.values());
      queries.getBatchChildren(batch.id()).items().forEach(job -> SUBMITTED.add(job.id()));
      check(
          queries.getBatchChildren(batch.id()).items().size() == 2,
          "missing persisted batch children");
      await(
          () -> SUBMITTED.stream().allMatch(id -> hasStatus(queries, id, JobStatus.SUCCEEDED)),
          "original feature jobs did not durably succeed: " + SUBMITTED);
      await(
          () -> hasStatus(queries, failed.id(), JobStatus.FAILED),
          "job did not enter dead-letter state");
      check(RETRIES.get() == 2 && FAILURES.get() == 2, "incorrect retry/dead-letter attempt count");
      var retryHistory = queries.getExecutionHistory(retried.id());
      var failedHistory = queries.getExecutionHistory(failed.id());
      check(
          retryHistory.size() == 2
              && !retryHistory.get(0).succeeded()
              && retryHistory.get(1).succeeded(),
          "retry history did not retain failure then success: " + retryHistory);
      check(
          failedHistory.size() == 2 && failedHistory.stream().noneMatch(run -> run.succeeded()),
          "dead-letter history was not durable: " + failedHistory);
      check(
          queries
              .getJobDetail(failed.id())
              .orElseThrow()
              .summary()
              .lastError()
              .contains("native permanent failure"),
          "dead-letter error missing");
      System.out.println("RATCHET_NATIVE_DURABILITY_VERIFIED " + SUBMITTED.size());
      System.out.println("RATCHET_NATIVE_FEATURES_VERIFIED " + EXECUTED);
    };
  }

  public static JobHandle track(JobHandle job) {
    SUBMITTED.add(job.id());
    return job;
  }

  public static void record(String value) {
    EXECUTIONS.putIfAbsent(value, JobContext.current().jobId());
    EXECUTED.add(value);
  }

  public static boolean hasStatus(JobQueryService queries, UUID id, JobStatus status) {
    return queries.getJobDetail(id).map(job -> job.summary().status() == status).orElse(false);
  }

  public static void await(BooleanSupplier condition, String message) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(50);
    check(condition.getAsBoolean(), message + "; actual=" + EXECUTED);
  }

  public static void check(boolean condition, String message) {
    if (!condition) throw new IllegalStateException(message);
  }

  public record RecordArgument(String value) implements Serializable {}

  public static class PojoArgument implements Serializable {
    private String value;

    public PojoArgument() {}

    public PojoArgument(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  public static class BaseJobs {
    public void record(String value) {
      NativeVerification.record(value);
    }
  }

  public static class Jobs extends BaseJobs {
    public void reference() {
      record("reference");
    }

    public void lookupReference() {
      record("lookup-reference");
    }

    public void recordArgument(RecordArgument argument) {
      record(argument.value());
    }

    public void pojoArgument(PojoArgument argument) {
      record(argument.getValue());
    }

    public String result() {
      return "result-value";
    }

    public static Boolean validResult(String value) {
      return "result-value".equals(value);
    }

    public void workflow() {
      record("workflow");
    }

    public void encrypted(String secret) {
      check("native-secret-argument".equals(secret), "encrypted argument did not round-trip");
      record("encrypted");
    }

    public void retryOnce() {
      if (RETRIES.incrementAndGet() == 1)
        throw new IllegalStateException("native transient failure");
      record("retry");
    }

    public void alwaysFail() {
      FAILURES.incrementAndGet();
      throw new IllegalStateException("native permanent failure");
    }

    public void signal() {
      check("delivered".equals(JobContext.current().signalPayload(String.class)), "signal payload");
      record("signal");
    }

    public void virtualThread() {
      check(Thread.currentThread().toString().startsWith("VirtualThread"), "not a virtual thread");
      record("virtual");
    }
  }

  public static class RecurringBase {}

  public static class RecurringJobs extends RecurringBase {
    @Recurring(cron = "0/1 * * * * ?", id = "native-verification-recurring")
    public void recurring() {
      NativeVerification.record("recurring");
    }
  }

  public static class LazyJob {
    public LazyJob() {
      CONSTRUCTED.incrementAndGet();
    }

    public void execute() {
      NativeVerification.record("lazy");
    }
  }

  public static class PrototypeJob {
    public PrototypeJob() {
      CONSTRUCTED.incrementAndGet();
    }

    public void execute() {
      NativeVerification.record("prototype");
    }

    @PreDestroy
    public void close() {
      DESTROYED.incrementAndGet();
    }
  }

  static class NestedSubmitter {
    private final ApplicationContext context;

    NestedSubmitter(ApplicationContext context) {
      this.context = context;
    }

    void submit(Jobs jobs, String value) {
      track(context.getBean(JobSchedulerService.class).enqueueNow(() -> jobs.record(value)));
    }
  }

  static class SubmitterBase {
    final ObjectProvider<JobSchedulerService> provider;

    SubmitterBase(ObjectProvider<JobSchedulerService> provider) {
      this.provider = provider;
    }

    void submit(Jobs jobs) {
      track(provider.getObject().enqueueNow(() -> jobs.record("inherited")));
    }
  }

  static class InheritedSubmitter extends SubmitterBase {
    InheritedSubmitter(ObjectProvider<JobSchedulerService> provider) {
      super(provider);
    }
  }
}
