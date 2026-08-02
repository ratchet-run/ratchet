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
package run.ratchet.spring.boot.it.nativepostgresql;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.springframework.aot.AotDetector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.ExecutionHistorySummary;
import run.ratchet.api.JobDetail;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.internal.JobPayloadInvoker;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.postgresql.PostgresqlJobStore;

/** Runs and independently records the twelve real native PostgreSQL qualification scenarios. */
@Component
public class NativePostgresqlScenarios {

  static final List<String> REQUIRED_SCENARIOS =
      List.of(
          "schema-migration",
          "node-registration",
          "direct-submission",
          "method-reference-submission",
          "wrapper-submission",
          "persistence-claim",
          "retry-history",
          "recurring-execution",
          "jsonb-round-trip",
          "class-policy-creation-denial",
          "class-policy-invocation-denial",
          "clean-shutdown");

  private static final Duration JOB_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration RETRY_TIMEOUT = Duration.ofSeconds(45);
  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(35);
  private static final int RETRY_FAILURES = 2;
  private static final String POLICY_DENIAL_MESSAGE = "is not allowed for job execution";
  private static final String AOT_MANIFEST = "META-INF/ratchet/aot-registered-classes.txt";

  private final ConfigurableApplicationContext applicationContext;
  private final JdbcTemplate jdbcTemplate;
  private final JobSchedulerService scheduler;
  private final JobQueryService queryService;
  private final PostgresqlJobStore store;
  private final JobPayloadInvoker payloadInvoker;
  private final NativePostgresqlJobs jobs;
  private final DeniedJob deniedJob;

  public NativePostgresqlScenarios(
      ConfigurableApplicationContext applicationContext,
      JdbcTemplate jdbcTemplate,
      JobSchedulerService scheduler,
      JobQueryService queryService,
      PostgresqlJobStore store,
      JobPayloadInvoker payloadInvoker,
      NativePostgresqlJobs jobs,
      DeniedJob deniedJob) {
    this.applicationContext = applicationContext;
    this.jdbcTemplate = jdbcTemplate;
    this.scheduler = scheduler;
    this.queryService = queryService;
    this.store = store;
    this.payloadInvoker = payloadInvoker;
    this.jobs = jobs;
    this.deniedJob = deniedJob;
  }

  /** Runs every scenario once, preserving the evidence order required by the native harness. */
  public List<Evidence> runAll() {
    jobs.reset();
    List<Evidence> evidence = new ArrayList<>(REQUIRED_SCENARIOS.size());
    evidence.add(runSchemaMigration());
    evidence.add(runNodeRegistration());
    evidence.add(runDirectSubmission());
    evidence.add(runMethodReferenceSubmission());
    evidence.add(runWrapperSubmission());
    evidence.add(runPersistenceClaim());
    evidence.add(runRetryHistory());
    evidence.add(runRecurringExecution());
    evidence.add(runJsonbRoundTrip());
    evidence.add(runClassPolicyCreationDenial());
    evidence.add(runClassPolicyInvocationDenial());
    evidence.add(runCleanShutdown());
    return List.copyOf(evidence);
  }

  Evidence runSchemaMigration() {
    return capture(
        "schema-migration",
        () -> {
          Long applied =
              jdbcTemplate.queryForObject(
                  "SELECT COUNT(*) FROM ratchet_schema_version", Long.class);
          Long schedulerTable =
              jdbcTemplate.queryForObject(
                  "SELECT COUNT(*) FROM information_schema.tables"
                      + " WHERE table_schema = 'public' AND table_name = 'scheduler_job'",
                  Long.class);
          require(applied != null && applied > 0, "no Ratchet schema migrations were recorded");
          require(
              schedulerTable != null && schedulerTable == 1,
              "scheduler_job was not created by migration");
          return applied + " migrations applied to an empty PostgreSQL database";
        });
  }

  Evidence runNodeRegistration() {
    return capture(
        "node-registration",
        () -> {
          var nodes = store.findAllNodes(10);
          require(!nodes.isEmpty(), "the scheduler did not persist a node heartbeat");
          require(store.countActiveNodes() > 0, "the scheduler has no active registered node");
          require(
              nodes.get(0).getLastHeartbeat() != null,
              "the registered node has no heartbeat timestamp");
          return "registered node " + nodes.get(0).getId() + " has an active heartbeat";
        });
  }

  Evidence runDirectSubmission() {
    return capture(
        "direct-submission",
        () -> {
          JobHandle handle = scheduler.enqueueNow(jobs::directJob);
          awaitSucceeded(handle, JOB_TIMEOUT);
          require(jobs.directExecutions() == 1, "the component job did not execute exactly once");
          return "component method completed as job " + handle.id();
        });
  }

  Evidence runMethodReferenceSubmission() {
    return capture(
        "method-reference-submission",
        () -> {
          JobHandle bound = scheduler.enqueueNow(jobs::boundReferenceJob);
          JobHandle staticReference =
              scheduler.enqueueNow(NativePostgresqlJobs::staticReferenceJob);
          awaitSucceeded(bound, JOB_TIMEOUT);
          awaitSucceeded(staticReference, JOB_TIMEOUT);
          require(
              jobs.boundReferenceExecutions() == 1,
              "the bound method reference did not execute exactly once");
          require(
              jobs.staticReferenceExecutions() == 1,
              "the static method reference did not execute exactly once");
          return "bound " + bound.id() + " and static " + staticReference.id() + " completed";
        });
  }

  Evidence runWrapperSubmission() {
    return capture(
        "wrapper-submission",
        () -> {
          String capturedString = "native-wrapper";
          NativePostgresqlJobs.WrapperCapture capturedRecord =
              new NativePostgresqlJobs.WrapperCapture("record", 17);
          JobHandle handle =
              scheduler.enqueueNow(
                  () -> NativePostgresqlJobs.wrapperJob(capturedString, capturedRecord));
          awaitSucceeded(handle, JOB_TIMEOUT);
          require(
              "native-wrapper:record:17".equals(jobs.wrapperObservation()),
              "the inline wrapper did not restore its String and record captures");
          return "inline wrapper captures completed as job " + handle.id();
        });
  }

  Evidence runPersistenceClaim() {
    return capture(
        "persistence-claim",
        () -> {
          JobHandle handle = scheduler.enqueueNow(jobs::persistenceJob);
          JobDetail detail = awaitSucceeded(handle, JOB_TIMEOUT);
          List<ExecutionHistorySummary> history =
              awaitHistory(
                  handle.id(),
                  attempts ->
                      attempts.size() == 1
                          && attempts.get(0).succeeded()
                          && attempts.get(0).endedAt() != null,
                  "one completed execution",
                  JOB_TIMEOUT);
          ExecutionHistorySummary execution = history.get(0);
          require(detail.summary().id().equals(handle.id()), "the persisted job id changed");
          require(
              detail.summary().status() == JobStatus.SUCCEEDED,
              "the persisted job did not record completion");
          require(
              execution.nodeId() != null && !execution.nodeId().isBlank(),
              "the execution history did not record the claiming node");
          require(
              execution.startedAt() != null && execution.endedAt() != null,
              "the execution history did not record a completed claim");
          require(jobs.persistenceExecutions() == 1, "the persisted job did not execute once");
          return "job "
              + handle.id()
              + " persisted, claimed by "
              + execution.nodeId()
              + ", and completed";
        });
  }

  Evidence runRetryHistory() {
    return capture(
        "retry-history",
        () -> {
          JobHandle handle =
              scheduler
                  .enqueue(jobs::retryJob)
                  .withMaxRetries(RETRY_FAILURES)
                  .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
                  .submit();
          JobDetail terminal = awaitSucceeded(handle, RETRY_TIMEOUT);
          List<ExecutionHistorySummary> history =
              awaitHistory(
                  handle.id(),
                  attempts ->
                      attempts.size() == RETRY_FAILURES + 1
                          && attempts.stream().filter(attempt -> !attempt.succeeded()).count()
                              == RETRY_FAILURES
                          && attempts.get(attempts.size() - 1).succeeded()
                          && attempts.get(attempts.size() - 1).endedAt() != null,
                  RETRY_FAILURES + " failed attempts followed by one completed attempt",
                  RETRY_TIMEOUT);
          long failedAttempts = history.stream().filter(attempt -> !attempt.succeeded()).count();
          require(
              failedAttempts == RETRY_FAILURES,
              "expected " + RETRY_FAILURES + " failed retry records but found " + failedAttempts);
          require(
              history.size() == RETRY_FAILURES + 1,
              "retry history must contain the failed attempts plus the final success");
          require(
              history.get(history.size() - 1).succeeded(),
              "the final retry history entry was not successful");
          require(
              terminal.summary().attempts() == RETRY_FAILURES,
              "the terminal job did not retain the configured retry count");
          for (int index = 0; index < history.size(); index++) {
            require(
                history.get(index).attempt() == index + 1,
                "retry history attempt numbers are not contiguous");
          }
          require(
              jobs.retryAttempts() == RETRY_FAILURES + 1,
              "the retry target did not execute the expected number of times");
          return RETRY_FAILURES + " retries recorded before job " + handle.id() + " succeeded";
        });
  }

  Evidence runRecurringExecution() {
    return capture(
        "recurring-execution",
        () -> {
          JobHandle master =
              scheduler.scheduleRecurringUtc("*/1 * * * * ?", jobs::recurringJob).submit();
          boolean canceled = false;
          try {
            awaitCondition(
                () -> jobs.recurringExecutions() >= 1,
                JOB_TIMEOUT,
                "the recurring schedule did not fire");
          } finally {
            canceled = scheduler.cancelJob(master.id());
          }
          require(canceled, "the recurring master could not be canceled");
          awaitCondition(
              () ->
                  queryService.getRecurringMasters().items().stream()
                      .noneMatch(job -> master.id().equals(job.id())),
              JOB_TIMEOUT,
              "the canceled recurring master remained active");
          return "recurring master " + master.id() + " produced an execution";
        });
  }

  Evidence runJsonbRoundTrip() {
    return capture(
        "jsonb-round-trip",
        () -> {
          NativePostgresqlPayload expected = new NativePostgresqlPayload("native-jsonb", 29);
          JobHandle handle = scheduler.enqueueNow(() -> jobs.jsonbJob(expected));
          awaitSucceeded(handle, JOB_TIMEOUT);
          require(
              expected.equals(jobs.receivedPayload()),
              "the PostgreSQL payload did not round-trip through JSON-B: "
                  + jobs.receivedPayload());
          return "DTO payload round-tripped through job " + handle.id();
        });
  }

  Evidence runClassPolicyCreationDenial() {
    return capture(
        "class-policy-creation-denial",
        () -> {
          assertDeniedJobIsAotRegistered();
          try {
            scheduler.enqueueNow(deniedJob::run);
          } catch (Throwable expected) {
            assertPolicyDenial(expected);
            return "creation rejected reachable target " + DeniedJob.class.getName();
          }
          throw new AssertionError("ClassPolicy unexpectedly allowed denied job creation");
        });
  }

  Evidence runClassPolicyInvocationDenial() {
    return capture(
        "class-policy-invocation-denial",
        () -> {
          assertDeniedJobIsAotRegistered();
          JobPayload payload =
              new JobPayload(DeniedJob.class.getName(), "run", "()V", false, List.of());
          try {
            payloadInvoker.invoke(payload);
          } catch (Throwable expected) {
            assertPolicyDenial(expected);
            return "invocation rejected reachable target " + DeniedJob.class.getName();
          }
          throw new AssertionError("ClassPolicy unexpectedly allowed denied job invocation");
        });
  }

  Evidence runCleanShutdown() {
    return capture(
        "clean-shutdown",
        () -> {
          SmartLifecycle lifecycle =
              applicationContext.getBean("ratchetLifecycle", SmartLifecycle.class);
          require(lifecycle.isRunning(), "Ratchet lifecycle was not running before shutdown");
          jobs.prepareBlockingJob();
          JobHandle handle = scheduler.enqueueNow(jobs::blockingJob);
          require(
              jobs.awaitBlockingStarted(JOB_TIMEOUT),
              "the shutdown probe job did not enter execution");
          Thread releaser =
              new Thread(
                  () -> {
                    try {
                      Thread.sleep(300);
                    } catch (InterruptedException failure) {
                      Thread.currentThread().interrupt();
                    } finally {
                      jobs.releaseBlockingJob();
                    }
                  },
                  "native-postgresql-shutdown-release");
          releaser.setDaemon(true);
          releaser.start();
          Instant started = Instant.now();
          Duration elapsed;
          boolean completedAtClose;
          try {
            applicationContext.close();
            elapsed = Duration.between(started, Instant.now());
            completedAtClose = jobs.blockingCompleted();
          } finally {
            jobs.releaseBlockingJob();
            releaser.join(5000);
          }
          require(!lifecycle.isRunning(), "Ratchet lifecycle remained running after stop");
          require(completedAtClose, "context close did not drain the in-flight job");
          require(
              elapsed.compareTo(Duration.ofMillis(200)) >= 0,
              "context close returned before the in-flight job was released: " + elapsed);
          require(
              elapsed.compareTo(SHUTDOWN_TIMEOUT) < 0,
              "bounded shutdown exceeded " + SHUTDOWN_TIMEOUT + ": " + elapsed);
          return "context close drained in-flight job "
              + handle.id()
              + " in "
              + elapsed.toMillis()
              + " ms";
        });
  }

  private JobDetail awaitSucceeded(JobHandle handle, Duration timeout) {
    Instant deadline = Instant.now().plus(timeout);
    JobStatus lastStatus = null;
    while (Instant.now().isBefore(deadline)) {
      Optional<JobDetail> current = queryService.getJobDetail(handle.id());
      if (current.isPresent()) {
        JobDetail detail = current.get();
        lastStatus = detail.summary().status();
        if (lastStatus == JobStatus.SUCCEEDED) {
          return detail;
        }
        if (lastStatus == JobStatus.FAILED || lastStatus == JobStatus.CANCELED) {
          throw new AssertionError("job " + handle.id() + " reached terminal status " + lastStatus);
        }
      }
      sleepBriefly();
    }
    throw new AssertionError(
        "job " + handle.id() + " did not succeed within " + timeout + "; last=" + lastStatus);
  }

  private List<ExecutionHistorySummary> awaitHistory(
      UUID jobId,
      Predicate<List<ExecutionHistorySummary>> complete,
      String expectation,
      Duration timeout) {
    Instant deadline = Instant.now().plus(timeout);
    List<ExecutionHistorySummary> history = List.of();
    while (Instant.now().isBefore(deadline)) {
      history = queryService.getExecutionHistory(jobId);
      if (complete.test(history)) {
        return history;
      }
      sleepBriefly();
    }
    throw new AssertionError(
        "job "
            + jobId
            + " history did not reach "
            + expectation
            + " within "
            + timeout
            + "; found="
            + history.size());
  }

  private static void awaitCondition(
      BooleanSupplier condition, Duration timeout, String failureMessage) {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      if (condition.getAsBoolean()) {
        return;
      }
      sleepBriefly();
    }
    throw new AssertionError(failureMessage + " within " + timeout);
  }

  private static void sleepBriefly() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while awaiting scheduler evidence", failure);
    }
  }

  private static void assertPolicyDenial(Throwable failure) {
    String messages = failureMessages(failure);
    String expectedMessage =
        "Class " + DeniedJob.class.getName() + " " + POLICY_DENIAL_MESSAGE + ".";
    boolean corePolicyFailure = false;
    boolean missingReflectionFailure = false;
    Throwable current = failure;
    while (current != null) {
      corePolicyFailure |= expectedMessage.equals(current.getMessage());
      missingReflectionFailure |=
          current.getClass().getName().contains("MissingReflectionRegistrationError");
      current = current.getCause();
    }
    require(
        corePolicyFailure,
        "ClassPolicy denial did not contain the exact core policy message '"
            + expectedMessage
            + "': "
            + messages);
    require(
        !missingReflectionFailure,
        "ClassPolicy denial incorrectly reached missing reflection metadata: " + messages);
  }

  private static void assertDeniedJobIsAotRegistered() throws IOException {
    if (!AotDetector.useGeneratedArtifacts()) {
      return;
    }
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try (InputStream manifest = classLoader.getResourceAsStream(AOT_MANIFEST)) {
      require(manifest != null, "Ratchet AOT manifest is missing from the native application");
      String contents = new String(manifest.readAllBytes(), StandardCharsets.UTF_8);
      require(
          contents.lines().anyMatch(DeniedJob.class.getName()::equals),
          "DeniedJob is absent from the generated Ratchet AOT manifest");
    }
  }

  private static String failureMessages(Throwable failure) {
    StringBuilder messages = new StringBuilder();
    Throwable current = failure;
    while (current != null) {
      if (!messages.isEmpty()) {
        messages.append(" | ");
      }
      messages.append(current.getClass().getName()).append(": ").append(current.getMessage());
      current = current.getCause();
    }
    return messages.toString();
  }

  private static Evidence capture(String scenario, Scenario body) {
    try {
      return Evidence.passed(scenario, body.run());
    } catch (Throwable failure) {
      return Evidence.failed(scenario, failure);
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  @FunctionalInterface
  private interface Scenario {
    String run() throws Exception;
  }

  /** One scenario verdict emitted identically by the JVM control and native executable. */
  public record Evidence(String scenario, boolean passed, String detail) {

    private static Evidence passed(String scenario, String detail) {
      return new Evidence(scenario, true, detail);
    }

    private static Evidence failed(String scenario, Throwable failure) {
      return new Evidence(
          scenario, false, failure.getClass().getName() + ": " + failure.getMessage());
    }

    public String toJson() {
      return "{\"scenario\":\""
          + escape(scenario)
          + "\",\"passed\":"
          + passed
          + ",\"detail\":\""
          + escape(detail)
          + "\"}";
    }

    private static String escape(String value) {
      return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
  }
}
