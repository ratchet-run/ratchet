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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RecurringMisfirePolicy;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

class DefaultJobCreationServiceRecurringReconciliationTest {

  private static final Instant START = Instant.parse("2026-05-27T12:00:00Z");
  private static final String BUSINESS_KEY = "invoice-rollup";

  public static void noopTask() {}

  @Test
  void submitRecurring_reusesExistingMaster_whenBusinessKeyIsRegisteredAgain() {
    FakeRecurringJobStore store = new FakeRecurringJobStore();
    MutableClock clock = new MutableClock(START);
    DefaultJobCreationService service = newService(store, clock);

    UUID firstId =
        submit(service, "0 * * * * ?", ZoneOffset.UTC, JobOptions.defaults(), BUSINESS_KEY).id();
    clock.advance(Duration.ofMinutes(30));

    UUID secondId =
        submit(service, "0 * * * * ?", ZoneOffset.UTC, JobOptions.defaults(), BUSINESS_KEY).id();

    assertEquals(firstId, secondId);
    assertEquals(1, store.size());
    assertEquals(1, store.createCount());
  }

  @Test
  void submitRecurring_preservesNextFire_whenBusinessKeyIsRegisteredAgainUnchanged() {
    FakeRecurringJobStore store = new FakeRecurringJobStore();
    MutableClock clock = new MutableClock(START);
    DefaultJobCreationService service = newService(store, clock);

    submit(service, "0 * * * * ?", ZoneOffset.UTC, JobOptions.defaults(), BUSINESS_KEY);
    Instant originalNextFire = store.onlyDefinition().nextFire();
    clock.advance(Duration.ofMinutes(30));

    submit(service, "0 * * * * ?", ZoneOffset.UTC, JobOptions.defaults(), BUSINESS_KEY);

    assertEquals(originalNextFire, store.onlyDefinition().nextFire());
    assertEquals(0, store.updateCount());
  }

  @Test
  void submitRecurring_updatesExistingMaster_whenCronZoneOrOptionsChange() {
    FakeRecurringJobStore store = new FakeRecurringJobStore();
    MutableClock clock = new MutableClock(START);
    DefaultJobCreationService service = newService(store, clock);
    JobOptions updatedOptions =
        JobOptions.defaults()
            .withPriority(JobPriority.HIGH)
            .withMaxRetries(3)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofSeconds(5))
            .withTimeout(Duration.ofSeconds(30));

    UUID firstId =
        submit(service, "0 * * * * ?", ZoneOffset.UTC, JobOptions.defaults(), BUSINESS_KEY).id();
    UUID secondId =
        submit(service, "0 30 9 * * ?", ZoneId.of("America/New_York"), updatedOptions, BUSINESS_KEY)
            .id();

    RecurringJobDefinition updated = store.onlyDefinition();
    assertEquals(firstId, secondId);
    assertEquals(1, store.size());
    assertEquals(1, store.updateCount());
    assertEquals("0 30 9 * * ?", updated.cronExpr());
    assertEquals("America/New_York", updated.zoneId());
    assertEquals(Instant.parse("2026-05-27T13:30:00Z"), updated.nextFire());
    assertEquals(JobPriority.HIGH.persistedCode(), updated.priority());
    assertEquals(3, updated.maxRetries());
    assertEquals(BackoffPolicy.FIXED, updated.backoffPolicy());
    assertEquals(5_000, updated.backoffParamMs());
    assertEquals(30, updated.timeoutSec());
  }

  @Test
  void submitRecurringPersistsAndReconcilesMisfirePolicyWithoutResettingSchedule() {
    FakeRecurringJobStore store = new FakeRecurringJobStore();
    MutableClock clock = new MutableClock(START);
    DefaultJobCreationService service = newService(store, clock);

    submit(
        service,
        "0 * * * * ?",
        ZoneOffset.UTC,
        JobOptions.defaults(),
        BUSINESS_KEY,
        RecurringMisfirePolicy.skip());
    Instant originalNextFire = store.onlyDefinition().nextFire();

    submit(
        service,
        "0 * * * * ?",
        ZoneOffset.UTC,
        JobOptions.defaults(),
        BUSINESS_KEY,
        RecurringMisfirePolicy.fireOnce());

    assertEquals(RecurringMisfirePolicy.fireOnce(), store.onlyDefinition().misfirePolicy());
    assertEquals(originalNextFire, store.onlyDefinition().nextFire());
    assertEquals(1, store.updateCount());
  }

  @Test
  void submitRecurring_keepsBusinessKeyConflict_whenSingleJobOwnsReservation() {
    FakeRecurringJobStore store = new FakeRecurringJobStore();
    store.reserveForSingleJob(BUSINESS_KEY);
    DefaultJobCreationService service = newService(store, new MutableClock(START));

    assertThrows(
        RatchetTransientStoreException.class,
        () -> submit(service, "0 * * * * ?", ZoneOffset.UTC, JobOptions.defaults(), BUSINESS_KEY));
    assertEquals(0, store.size());
  }

  @Test
  void submitRecurring_reconcilesAfterCreateRace_whenBusinessKeyInsertLoses() {
    FakeRecurringJobStore store = new FakeRecurringJobStore();
    MutableClock clock = new MutableClock(START);
    DefaultJobCreationService service = newService(store, clock);
    RecurringJobDefinition winner =
        existingDefinition(UUID.randomUUID(), BUSINESS_KEY, "0 0 8 * * ?", START.plusSeconds(120));
    store.failNextCreateWithRecurringWinner(winner);

    UUID id =
        submit(service, "0 * * * * ?", ZoneOffset.UTC, JobOptions.defaults(), BUSINESS_KEY).id();

    assertEquals(winner.id(), id);
    assertEquals(1, store.size());
    assertEquals(1, store.updateCount());
    assertEquals("0 * * * * ?", store.onlyDefinition().cronExpr());
  }

  private static JobHandle submit(
      DefaultJobCreationService service,
      String cron,
      ZoneId zone,
      JobOptions options,
      String businessKey) {
    return submit(service, cron, zone, options, businessKey, RecurringMisfirePolicy.defaults());
  }

  private static JobHandle submit(
      DefaultJobCreationService service,
      String cron,
      ZoneId zone,
      JobOptions options,
      String businessKey,
      RecurringMisfirePolicy misfirePolicy) {
    DefaultRecurringJobBuilder builder =
        new DefaultRecurringJobBuilder(
            cron, zone, DefaultJobCreationServiceRecurringReconciliationTest::noopTask, service);
    builder.withOptions(options);
    builder.withBusinessKey(businessKey);
    builder.withMisfirePolicy(misfirePolicy);
    return builder.submit();
  }

  private static DefaultJobCreationService newService(
      RecurringJobStore recurringJobStore, Clock clock) {
    return new DefaultJobCreationService(
        mock(JobBatchStatusStore.class),
        mock(JobTerminalStore.class),
        mock(JobCrudStore.class),
        mock(JobBulkStore.class),
        mock(BatchStore.class),
        mock(TagStore.class),
        mock(WorkflowConditionStore.class),
        recurringJobStore,
        new NoopJobWakeupService(),
        mock(RecurringScheduler.class),
        new DefaultJobInvocationResolver(),
        new JobPayloadInputValidator(),
        null,
        null,
        null,
        null,
        null,
        null,
        clock);
  }

  private static RecurringJobDefinition existingDefinition(
      UUID id, String businessKey, String cron, Instant nextFire) {
    JobOptions options = JobOptions.defaults();
    return new RecurringJobDefinition(
        id,
        cron,
        "UTC",
        nextFire,
        false,
        null,
        options.priority().persistedCode(),
        options.maxRetries(),
        options.backoffPolicy(),
        (int) options.backoffParam().toMillis(),
        options.timeoutSec(),
        new JobPayload(
            DefaultJobCreationServiceRecurringReconciliationTest.class.getName(),
            "noopTask",
            "()V",
            true,
            List.of()),
        null,
        null,
        businessKey,
        null,
        null,
        START,
        null,
        false,
        RecurringMisfirePolicy.defaults());
  }

  private static final class NoopJobWakeupService extends JobWakeupService {
    @Override
    public void notify(JobPriority priority, boolean immediate, String executionTarget) {}

    @Override
    public void notifyIfNeeded(
        JobExecutionType jobType, JobPriority priority, Duration delay, String executionTarget) {}
  }

  private static final class MutableClock extends Clock {
    private Instant instant;

    private MutableClock(Instant instant) {
      this.instant = instant;
    }

    private void advance(Duration duration) {
      instant = instant.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return Clock.fixed(instant, zone);
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }

  private static final class FakeRecurringJobStore implements RecurringJobStore {
    private final Map<String, RecurringJobDefinition> definitionsByBusinessKey =
        new LinkedHashMap<>();
    private final Set<String> singleJobBusinessKeys = new java.util.HashSet<>();

    private RecurringJobDefinition raceWinner;
    private int createCount;
    private int updateCount;

    private int size() {
      return definitionsByBusinessKey.size();
    }

    private int createCount() {
      return createCount;
    }

    private int updateCount() {
      return updateCount;
    }

    private RecurringJobDefinition onlyDefinition() {
      return new ArrayList<>(definitionsByBusinessKey.values()).get(0);
    }

    private void reserveForSingleJob(String businessKey) {
      singleJobBusinessKeys.add(businessKey);
    }

    private void failNextCreateWithRecurringWinner(RecurringJobDefinition definition) {
      raceWinner = definition;
    }

    @Override
    public UUID createRecurring(RecurringJobDefinition definition) {
      createCount++;
      String businessKey = definition.businessKey();
      if (raceWinner != null) {
        definitionsByBusinessKey.put(raceWinner.businessKey(), raceWinner);
        raceWinner = null;
        throw activeBusinessKey(definition.id());
      }
      if (singleJobBusinessKeys.contains(businessKey)
          || definitionsByBusinessKey.containsKey(businessKey)) {
        throw activeBusinessKey(definition.id());
      }
      definitionsByBusinessKey.put(businessKey, definition);
      return definition.id();
    }

    @Override
    public boolean updateRecurring(UUID id, RecurringJobDefinition definition) {
      Optional<RecurringJobDefinition> existing = getRecurring(id);
      if (existing.isEmpty()) {
        return false;
      }
      updateCount++;
      definitionsByBusinessKey.put(definition.businessKey(), definition);
      return true;
    }

    @Override
    public Optional<RecurringJobDefinition> getRecurring(UUID id) {
      return definitionsByBusinessKey.values().stream()
          .filter(definition -> definition.id().equals(id))
          .findFirst();
    }

    @Override
    public Optional<RecurringJobDefinition> findRecurringByBusinessKey(String businessKey) {
      return Optional.ofNullable(definitionsByBusinessKey.get(businessKey));
    }

    @Override
    public List<RecurringJobDefinition> listAll() {
      return List.copyOf(definitionsByBusinessKey.values());
    }

    @Override
    public List<RecurringJobDefinition> claimDueRecurring(
        int limit, String nodeId, run.ratchet.api.NodeTagFilter tagFilter) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void advanceNextFire(UUID id, Instant nextFire) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Instant> findEarliestRecurringNextFire() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean pauseRecurring(UUID id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean resumeRecurring(UUID id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancelRecurringAndArchive(UUID id, ArchiveReason reason) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int cancelOrphanedRecurringAnnotationJobs(
        Set<String> knownBusinessKeys, Instant nodeStartTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int cancelRecurringJobsByTag(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancelRecurringJobByBusinessKey(String businessKey) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int cancelRecurringJobsByBusinessKeys(Set<String> businessKeys) {
      throw new UnsupportedOperationException();
    }

    private static RatchetTransientStoreException activeBusinessKey(UUID id) {
      return new RatchetTransientStoreException(
          "Active business key in use for recurring master " + id, new RuntimeException("dup"));
    }
  }
}
