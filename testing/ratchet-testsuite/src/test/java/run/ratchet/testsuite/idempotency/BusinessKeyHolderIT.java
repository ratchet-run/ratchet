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
package run.ratchet.testsuite.idempotency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.BusinessKeyLookup;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Public reservation lookup inside and outside a caller transaction. */
@EnabledIfSystemProperty(
    named = "ratchet.test.db.type",
    matches = "mysql|postgresql|oracle|sqlserver")
class BusinessKeyHolderIT extends BaseRatchetIT {
  private static final Duration LATER = Duration.ofHours(1);
  private static final String YEARLY = "0 0 0 1 1 ?";

  @Inject private JobSchedulerService scheduler;
  @Inject private BusinessKeyLookup lookup;
  @Inject private UserTransaction utx;

  private final Set<UUID> jobs = new HashSet<>();
  private final Set<String> recurringKeys = new HashSet<>();

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @AfterEach
  void cleanup() throws Exception {
    if (utx.getStatus() != Status.STATUS_NO_TRANSACTION) {
      utx.rollback();
    }
    try {
      for (UUID id : jobs) {
        scheduler.cancelJob(id);
      }
    } finally {
      for (String key : recurringKeys) {
        scheduler.cancelRecurringJobByBusinessKey(key);
      }
    }
  }

  @Test
  void pendingJobIsVisibleUntilCanceled() {
    String key = uniqueKey();
    JobHandle handle = scheduler.schedule(LATER, SimpleJob::execute).withBusinessKey(key).submit();
    jobs.add(handle.id());
    var expected = new BusinessKeyHolder(handle.id(), JobType.SINGLE, JobStatus.PENDING);
    assertEquals(expected, scheduler.findBusinessKeyHolder(key).orElseThrow());
    assertEquals(expected, lookup.findBusinessKeyHolder(key).orElseThrow());
    scheduler.cancelJob(handle.id());
    assertTrue(lookup.findBusinessKeyHolder(key).isEmpty());
  }

  @Test
  void recurringMasterIsVisibleUntilCanceled() {
    String key = uniqueKey();
    JobHandle master = recurringHolder(key);
    var expected = new BusinessKeyHolder(master.id(), JobType.RECURRING, JobStatus.PENDING);
    assertEquals(expected, scheduler.findBusinessKeyHolder(key).orElseThrow());
    assertEquals(expected, lookup.findBusinessKeyHolder(key).orElseThrow());
    scheduler.cancelRecurringJobByBusinessKey(key);
    assertTrue(lookup.findBusinessKeyHolder(key).isEmpty());
  }

  @Test
  void stableLookupInjectionReturnsSchedulerAnswer() {
    String key = uniqueKey();
    JobHandle handle = scheduler.schedule(LATER, SimpleJob::execute).withBusinessKey(key).submit();
    jobs.add(handle.id());
    var expected = new BusinessKeyHolder(handle.id(), JobType.SINGLE, JobStatus.PENDING);
    assertEquals(expected, lookup.findBusinessKeyHolder(key).orElseThrow());
    assertEquals(scheduler.findBusinessKeyHolder(key), lookup.findBusinessKeyHolder(key));
  }

  @Test
  void lookupSeesCallerWritesAndInvalidKeysLeaveCallerCommittable() throws Exception {
    String recurringKey = uniqueKey();
    JobHandle master = recurringHolder(recurringKey);
    String ownKey = uniqueKey();
    utx.begin();
    var expected = new BusinessKeyHolder(master.id(), JobType.RECURRING, JobStatus.PENDING);
    assertEquals(expected, scheduler.findBusinessKeyHolder(recurringKey).orElseThrow());
    assertEquals(expected, lookup.findBusinessKeyHolder(recurringKey).orElseThrow());
    assertTrue(lookup.findBusinessKeyHolder("  ").isEmpty());
    assertTrue(lookup.findBusinessKeyHolder(uniqueKey()).isEmpty());
    assertThrows(
        IllegalArgumentException.class, () -> lookup.findBusinessKeyHolder("x".repeat(256)));
    assertThrows(IllegalArgumentException.class, () -> scheduler.findBusinessKeyHolder("café"));
    JobHandle own = scheduler.schedule(LATER, SimpleJob::execute).withBusinessKey(ownKey).submit();
    jobs.add(own.id());
    var ownHolder = new BusinessKeyHolder(own.id(), JobType.SINGLE, JobStatus.PENDING);
    assertEquals(ownHolder, lookup.findBusinessKeyHolder(ownKey).orElseThrow());
    assertEquals(Status.STATUS_ACTIVE, utx.getStatus());
    utx.commit();
    assertEquals(ownHolder, lookup.findBusinessKeyHolder(ownKey).orElseThrow());
  }

  private JobHandle recurringHolder(String key) {
    recurringKeys.add(key);
    return scheduler
        .scheduleRecurring(YEARLY, ZoneOffset.UTC, SimpleJob::execute)
        .withBusinessKey(key)
        .submit();
  }

  private static String uniqueKey() {
    return "holder-" + UUID.randomUUID();
  }
}
