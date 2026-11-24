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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import run.ratchet.store.spi.LockStore;

/** Keys retain their spelling and compare exactly under the binary column collation. */
class SqlserverExactKeyTest {
  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

  @BeforeEach
  @AfterEach
  void cleanup() {
    fixture.cleanupStore();
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
