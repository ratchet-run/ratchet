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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

/** Contract tests for {@code JobPropertyStore}. */
public abstract class AbstractJobPropertyStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupExtensionFixture() {
    cleanupStore();
  }

  // ---- Properties ----

  @Test
  void putProperty_roundTripsThroughGetProperty() {
    var job = persist(newPendingJob());

    propertyStore().putProperty(job.getId(), "ratchet-tck.block_name", "invoice.send");

    assertEquals(
        "invoice.send",
        propertyStore().getProperty(job.getId(), "ratchet-tck.block_name").orElseThrow());
  }

  @Test
  void putProperty_rejectsKeyLongerThan255Characters() {
    var job = persist(newPendingJob());

    assertThrows(
        IllegalArgumentException.class,
        () -> propertyStore().putProperty(job.getId(), "x".repeat(256), "value"));
  }

  @Test
  void putProperty_rejectsValueLongerThan1024Characters() {
    var job = persist(newPendingJob());

    assertThrows(
        IllegalArgumentException.class,
        () -> propertyStore().putProperty(job.getId(), "ratchet-tck.value", "x".repeat(1025)));
  }

  @Test
  void putProperty_roundTrips255CharacterKey() {
    var job = persist(newPendingJob());
    String key = "x".repeat(255);

    propertyStore().putProperty(job.getId(), key, "value");

    assertEquals("value", propertyStore().getProperty(job.getId(), key).orElseThrow());
  }

  @Test
  void putProperty_roundTrips1024CharacterValue() {
    var job = persist(newPendingJob());
    String value = "x".repeat(1024);

    propertyStore().putProperty(job.getId(), "ratchet-tck.value", value);

    assertEquals(
        value, propertyStore().getProperty(job.getId(), "ratchet-tck.value").orElseThrow());
  }

  @Test
  void putProperty_replacesExistingValueForSameKey() {
    var job = persist(newPendingJob());

    propertyStore().putProperty(job.getId(), "ratchet-tck.block_version", "1");
    propertyStore().putProperty(job.getId(), "ratchet-tck.block_version", "2");

    assertEquals(
        "2", propertyStore().getProperty(job.getId(), "ratchet-tck.block_version").orElseThrow());
  }

  @Test
  void getProperty_returnsEmptyWhenAbsent() {
    var job = persist(newPendingJob());

    assertTrue(propertyStore().getProperty(job.getId(), "ratchet-tck.missing").isEmpty());
    assertTrue(
        propertyStore().getProperty(new UUID(0L, 424_242L), "ratchet-tck.missing").isEmpty());
  }

  @Test
  void getPropertiesByPrefix_returnsOnlyMatchingNamespace() {
    var job = persist(newPendingJob());
    propertyStore().putProperty(job.getId(), "ratchet-tck.block_name", "invoice.send");
    propertyStore().putProperty(job.getId(), "ratchet-tck.block_version", "2");
    propertyStore().putProperty(job.getId(), "other-ext.key", "ignored");

    Map<String, String> scoped = propertyStore().getPropertiesByPrefix(job.getId(), "ratchet-tck.");

    assertEquals(
        Map.of("ratchet-tck.block_name", "invoice.send", "ratchet-tck.block_version", "2"), scoped);
  }

  @Test
  void getPropertiesByPrefix_emptyPrefixReturnsEverything() {
    var job = persist(newPendingJob());
    propertyStore().putProperty(job.getId(), "a.one", "1");
    propertyStore().putProperty(job.getId(), "b.two", "2");

    assertEquals(2, propertyStore().getPropertiesByPrefix(job.getId(), "").size());
  }

  @Test
  void getPropertiesByPrefix_treatsLikeMetacharactersLiterally() {
    var job = persist(newPendingJob());
    propertyStore().putProperty(job.getId(), "pct%key.x", "match");
    propertyStore().putProperty(job.getId(), "pctXkey.x", "no-match");

    Map<String, String> scoped = propertyStore().getPropertiesByPrefix(job.getId(), "pct%");

    assertEquals(Map.of("pct%key.x", "match"), scoped);
  }

  @Test
  void properties_areScopedToTheOwningJob() {
    var first = persist(newPendingJob());
    var second = persist(newPendingJob());
    propertyStore().putProperty(first.getId(), "ratchet-tck.block_name", "invoice.send");

    assertTrue(propertyStore().getProperty(second.getId(), "ratchet-tck.block_name").isEmpty());
  }

  @Test
  void archiveJob_copiesPropertiesOntoArchiveRow() {
    var job = persist(newPendingJob());
    propertyStore().putProperty(job.getId(), "ratchet-tck.block_name", "invoice.send");
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store()
        .markJobSucceeded(
            job.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();

    var archived = archiveStore().archiveJob(completed, "tck", "tck-node");

    assertTrue(
        archived.getProperties() != null
            && archived.getProperties().contains("ratchet-tck.block_name")
            && archived.getProperties().contains("invoice.send"),
        "archive row must carry the job's properties as JSON");
  }
}
