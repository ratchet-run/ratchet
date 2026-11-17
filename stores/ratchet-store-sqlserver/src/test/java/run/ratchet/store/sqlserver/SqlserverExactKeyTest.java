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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;

/** Keys retain their spelling and compare exactly under the binary column collation. */
class SqlserverExactKeyTest {
  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

  @BeforeEach
  @AfterEach
  void cleanup() {
    fixture.cleanupStore();
  }

  static Stream<Arguments> distinctKeys() {
    return Stream.of(
        Arguments.of("x-a", "X-A"),
        Arguments.of("x-a", "x-ā"),
        Arguments.of("x-ā", "x-a\u0304"),
        Arguments.of("x-😀", "x-😁"),
        Arguments.of("x-😀", "x-"));
  }

  @ParameterizedTest
  @MethodSource("distinctKeys")
  void locksAndNodesKeepBothKeys(String first, String second) {
    var store = fixture.store();
    var locks = store.capability(LockStore.class).orElseThrow();
    assertTrue(locks.tryLock(first, Duration.ofMinutes(5), "first-owner"));
    assertTrue(locks.tryLock(second, Duration.ofMinutes(5), "second-owner"));
    assertFalse(locks.tryLock(first, Duration.ofMinutes(5), "second-owner"));
    assertFalse(locks.tryLock(second, Duration.ofMinutes(5), "first-owner"));
    assertTrue(locks.renewLock(first, Duration.ofMinutes(5), "first-owner"));
    assertTrue(locks.renewLock(second, Duration.ofMinutes(5), "second-owner"));
    Instant now = Instant.now();
    store.upsertHeartbeat(first, now);
    store.upsertHeartbeat(second, now.plusSeconds(1));
    assertEquals(first, store.findNodeById(first).orElseThrow().getId());
    assertEquals(second, store.findNodeById(second).orElseThrow().getId());
    assertEquals(2, store.findAllNodes(10).size());
  }

  @ParameterizedTest
  @MethodSource("distinctKeys")
  void tagsSignalsAndBusinessKeysKeepBothJobs(String first, String second) {
    var store = fixture.store();
    var queries = store.capability(JobQueryStore.class).orElseThrow();
    var signals = store.capability(SignalStore.class).orElseThrow();
    var firstJob = fixture.newPendingJob();
    firstJob.setBusinessKey(first);
    var secondJob = fixture.newPendingJob();
    secondJob.setBusinessKey(second);
    store.create(firstJob);
    store.create(secondJob);
    assertEquals(firstJob.getId(), store.findBusinessKeyHolder(first).orElseThrow().id());
    assertEquals(secondJob.getId(), store.findBusinessKeyHolder(second).orElseThrow().id());
    assertEquals(first, store.findById(firstJob.getId()).orElseThrow().getBusinessKey());
    assertEquals(second, store.findById(secondJob.getId()).orElseThrow().getBusinessKey());
    store.insertTags(firstJob.getId(), List.of(first));
    store.insertTags(secondJob.getId(), List.of(second));
    assertEquals(List.of(firstJob.getId()), queries.findJobIdsByTag(first, 10, 0));
    assertEquals(List.of(secondJob.getId()), queries.findJobIdsByTag(second, 10, 0));
    assertEquals(List.of(first), store.findById(firstJob.getId()).orElseThrow().getTags());
    assertEquals(List.of(second), store.findById(secondJob.getId()).orElseThrow().getTags());

    var firstWaiting = fixture.newPendingJob();
    firstWaiting.setStatus(JobStatus.WAITING);
    firstWaiting.setSignalKey(first);
    firstWaiting.setSignalTimeout(Instant.now().plusSeconds(600));
    var secondWaiting = fixture.newPendingJob();
    secondWaiting.setStatus(JobStatus.WAITING);
    secondWaiting.setSignalKey(second);
    secondWaiting.setSignalTimeout(Instant.now().plusSeconds(600));
    store.create(firstWaiting);
    store.create(secondWaiting);
    assertEquals(first, store.findById(firstWaiting.getId()).orElseThrow().getSignalKey());
    assertEquals(second, store.findById(secondWaiting.getId()).orElseThrow().getSignalKey());
    assertEquals(
        1,
        signals.deliverSignalByKey(
            first,
            null,
            null,
            "APPROVED",
            null,
            "caller",
            Instant.now(),
            UUID.randomUUID().toString()));
    assertEquals(JobStatus.PENDING, store.findById(firstWaiting.getId()).orElseThrow().getStatus());
    assertEquals(
        JobStatus.WAITING, store.findById(secondWaiting.getId()).orElseThrow().getStatus());
    assertEquals(
        1,
        signals.deliverSignalByKey(
            second,
            null,
            null,
            "APPROVED",
            null,
            "caller",
            Instant.now(),
            UUID.randomUUID().toString()));
    assertEquals(
        JobStatus.PENDING, store.findById(secondWaiting.getId()).orElseThrow().getStatus());
  }

  @ParameterizedTest
  @MethodSource("distinctKeys")
  void resourcesKeepBothConfigurations(String first, String second) {
    var resources = fixture.store().capability(ResourcePermitStore.class).orElseThrow();
    resources.configureResource(first, 1, 100, "first");
    resources.configureResource(second, 2, 200, "second");
    var firstLimit = resources.findResourceLimit(first).orElseThrow();
    var secondLimit = resources.findResourceLimit(second).orElseThrow();
    assertEquals(first, firstLimit.resourceName());
    assertEquals(second, secondLimit.resourceName());
    assertEquals(1, firstLimit.maxConcurrent());
    assertEquals(2, secondLimit.maxConcurrent());
    assertEquals("first", firstLimit.description());
    assertEquals("second", secondLimit.description());
  }

  @Test
  void stringParametersSeekBinaryKeyIndexes() throws Exception {
    var store = fixture.store();
    var locks = store.capability(LockStore.class).orElseThrow();
    var job = store.create(fixture.newPendingJob());
    for (int i = 0; i < 500; i++) {
      assertTrue(locks.tryLock("seek-" + i, Duration.ofMinutes(5), "owner"));
      store.insertTags(job.getId(), List.of("seek-" + i));
    }
    try (var connection = fixture.openConnection()) {
      try (var statement = connection.createStatement()) {
        statement.execute("UPDATE STATISTICS scheduler_lock WITH FULLSCAN");
        statement.execute("UPDATE STATISTICS scheduler_job_tag WITH FULLSCAN");
        statement.execute("SET STATISTICS XML ON");
      }
      try {
        assertSeek(
            actualPlan(connection, "SELECT owner_node FROM scheduler_lock WHERE lock_name = ?"),
            "scheduler_lock",
            "pk_scheduler_lock",
            "Clustered Index Seek");
        assertSeek(
            actualPlan(connection, "SELECT job_id FROM scheduler_job_tag WHERE tag = ?"),
            "scheduler_job_tag",
            "idx_job_tag_tag_job",
            "Index Seek");
      } finally {
        try (var statement = connection.createStatement()) {
          statement.execute("SET STATISTICS XML OFF");
        }
      }
    }
  }

  private static String actualPlan(Connection connection, String sql) throws Exception {
    String plan = null;
    try (var statement = connection.prepareStatement(sql)) {
      statement.setString(1, "seek-250");
      boolean result = statement.execute();
      while (result || statement.getUpdateCount() != -1) {
        if (result) {
          try (var rows = statement.getResultSet()) {
            while (rows.next()) {
              String value = rows.getString(1);
              if (value != null && value.contains("<ShowPlanXML")) {
                plan = value;
              }
            }
          }
        }
        result = statement.getMoreResults();
      }
    }
    assertTrue(plan != null, "Expected an actual STATISTICS XML result for " + sql);
    return plan;
  }

  private static void assertSeek(String plan, String table, String index, String operation)
      throws Exception {
    var factory = DocumentBuilderFactory.newInstance();
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    var document = factory.newDocumentBuilder().parse(new InputSource(new StringReader(plan)));
    var operators = document.getElementsByTagName("RelOp");
    boolean found = false;
    for (int i = 0; i < operators.getLength(); i++) {
      var operator = (Element) operators.item(i);
      var objects = operator.getElementsByTagName("Object");
      for (int j = 0; j < objects.getLength(); j++) {
        var object = (Element) objects.item(j);
        if (!("[" + table + "]").equals(object.getAttribute("Table"))) continue;
        String physical = operator.getAttribute("PhysicalOp");
        assertFalse(physical.equals("Index Scan") || physical.equals("Clustered Index Scan"), plan);
        if (("[" + index + "]").equals(object.getAttribute("Index"))
            && operation.equals(physical)) {
          found = true;
        }
      }
    }
    assertTrue(found, "Expected " + operation + " on " + table + "." + index + ": " + plan);
  }
}
