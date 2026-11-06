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
package run.ratchet.testsuite.event;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.event.AbstractJobSchedulerEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.TerminalEventMapper;
import run.ratchet.ri.core.internal.TerminalEventRelay;
import run.ratchet.ri.core.internal.WorkflowCompletionPlan;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.spi.EventOutboxStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

class DurableTerminalEventsIT extends BaseRatchetIT {
  @Inject JobSchedulerService scheduler;
  @Inject TestJobService jobs;
  @Inject JobStore store;
  @Inject TerminalEventRelay relay;
  @Inject RatchetOptions options;
  @Inject OutboxProbe probe;

  @Deployment
  public static WebArchive deployment() {
    String db = System.getProperty("ratchet.test.db.type", "mysql");
    WebArchive archive =
        RatchetArchiveBuilder.create()
            .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), db)
            .addClasses(SimpleJob.class, FailingJob.class, TestJobService.class, OutboxProbe.class)
            .addStoreInfrastructure()
            .addBeansXml()
            .build();
    archive.addAsResource(
        new StringAsset(
            RatchetArchiveBuilder.testRuntimeConfigContent(
                db, Map.of("ratchet.events.durable", "true"))),
        "ratchet-testsuite.properties");
    return archive;
  }

  @Test
  void successAndFailureDeliverAndDeleteRows() {
    assertTrue(options.events().durable());
    List<AbstractJobSchedulerEvent> events = new CopyOnWriteArrayList<>();
    Set<UUID> observedRows = ConcurrentHashMap.newKeySet();
    Consumer<Object> listener =
        event -> {
          if (event instanceof JobCompletedEvent
              || event instanceof JobFailedEvent
              || event instanceof JobDlqEvent) {
            AbstractJobSchedulerEvent e = (AbstractJobSchedulerEvent) event;
            // Listeners run after the commit, where the finished JTA transaction cannot be
            // reused, so the probe reads in a transaction of its own.
            if (probe.contains(e.getEventId())) observedRows.add(e.getEventId());
            events.add(e);
          }
        };
    scheduler.addEventListener(listener);
    try {
      UUID success = jobs.enqueueNow(SimpleJob::execute).id();
      UUID failure = jobs.enqueue(FailingJob::execute).withMaxRetries(0).submit().id();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                assertEquals(
                    1,
                    events.stream()
                        .filter(e -> e.getJobId().equals(success) && e instanceof JobCompletedEvent)
                        .count());
                assertEquals(
                    1,
                    events.stream()
                        .filter(e -> e.getJobId().equals(failure) && e instanceof JobFailedEvent)
                        .count());
                assertEquals(
                    1,
                    events.stream()
                        .filter(e -> e.getJobId().equals(failure) && e instanceof JobDlqEvent)
                        .count());
                events.stream()
                    .filter(e -> e.getJobId().equals(success) || e.getJobId().equals(failure))
                    .forEach(
                        e -> {
                          assertNotNull(e.getEventId());
                          assertTrue(
                              observedRows.contains(e.getEventId()),
                              "Terminal event must be observed while its outbox row exists: "
                                  + e.getClass());
                        });
                assertTrue(
                    outbox().findEventsCreatedBefore(Instant.now().plusSeconds(60), 100).isEmpty());
              });
      Set<UUID> deliveredIds =
          events.stream()
              .filter(e -> e.getJobId().equals(success) || e.getJobId().equals(failure))
              .map(AbstractJobSchedulerEvent::getEventId)
              .collect(Collectors.toSet());
      await()
          .during(Duration.ofSeconds(2))
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  deliveredIds.forEach(
                      id ->
                          assertEquals(
                              1, events.stream().filter(e -> id.equals(e.getEventId())).count())));
      assertTrue(outbox().findEventsCreatedBefore(Instant.now().plusSeconds(60), 100).isEmpty());
    } finally {
      scheduler.removeEventListener(listener);
    }
  }

  @Test
  void relayDeliversOldRowAndDeletesIt() {
    UUID jobId = jobs.schedule(Duration.ofDays(1), SimpleJob::execute).submit().id();
    // A success transition needs a RUNNING job, as a worker would hold.
    assertTrue(store.tryPickUpJob(jobId, "durable-events-it"));
    var job = store.findById(jobId).orElseThrow();
    job.setStatus(JobStatus.SUCCEEDED);
    Instant old = Instant.now().minus(Duration.ofDays(1));
    job.setExecutionStartTime(old);
    job.setExecutionEndTime(old);
    job.setExecutionDurationMs(1L);
    JobCompletedEvent event =
        new JobCompletedEvent(
            UUID.randomUUID(),
            jobId,
            job.getBusinessKey(),
            job.getRecurringMasterId(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            old,
            1L);
    JobCompletionPlan plan =
        PostExecutionHandler.completionPlan(
            job, JobStatus.RUNNING, null, WorkflowCompletionPlan.empty());
    JobCompletionPlan withRow =
        new JobCompletionPlan(
            plan.jobId(),
            plan.expectedStatus(),
            plan.terminalStatus(),
            plan.resultJson(),
            plan.resultType(),
            plan.errorMessage(),
            plan.attempts(),
            plan.start(),
            plan.end(),
            plan.durationMs(),
            plan.queueWaitMs(),
            plan.batchId(),
            plan.completedBatch(),
            plan.dependencies(),
            plan.expectedClaimSeq(),
            List.of(TerminalEventMapper.toRow(event, old)));
    List<Object> seen = new CopyOnWriteArrayList<>();
    Consumer<Object> listener = seen::add;
    scheduler.addEventListener(listener);
    try {
      assertTrue(store.commitCompletion(withRow).committed());
      relay.relayNow();
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                assertTrue(
                    seen.stream()
                        .anyMatch(
                            e ->
                                e instanceof JobCompletedEvent c
                                    && c.getEventId().equals(event.getEventId())));
                assertTrue(outbox().findEventsCreatedBefore(Instant.now(), 100).isEmpty());
              });
    } finally {
      scheduler.removeEventListener(listener);
    }
  }

  private EventOutboxStore outbox() {
    return store.capability(EventOutboxStore.class).orElseThrow();
  }

  /** Reads the outbox in a new transaction, so an event listener can call it after the commit. */
  @ApplicationScoped
  public static class OutboxProbe {
    @Inject JobStore store;

    @Transactional(TxType.REQUIRES_NEW)
    public boolean contains(UUID eventId) {
      return store
          .capability(EventOutboxStore.class)
          .orElseThrow()
          .findEventsCreatedBefore(Instant.now().plusSeconds(3600), 100)
          .stream()
          .anyMatch(row -> row.eventId().equals(eventId));
    }
  }
}
