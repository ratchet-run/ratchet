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
package run.ratchet.spring.boot.it.sqlserver.fixture.tck;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.util.List;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Deletes all SQL Server scheduler data in foreign-key-safe order. */
public class SpringTckStoreCleaner {

  private static final List<String> SCHEDULER_TABLES =
      List.of(
          "scheduler_business_key_reservation",
          "scheduler_job_queue",
          "scheduler_job_tag",
          "scheduler_job_log",
          "scheduler_job_execution",
          "scheduler_resource_permit",
          "scheduler_workflow_condition",
          "scheduler_batch_metrics",
          "scheduler_job_archive",
          "scheduler_job_properties",
          "scheduler_job_extension_state",
          "scheduler_job",
          "scheduler_recurring_job_archive",
          "scheduler_recurring_job",
          "scheduler_batch",
          "scheduler_resource_limit",
          "scheduler_lock",
          "scheduler_node");

  @PersistenceContext private EntityManager entityManager;

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void truncateAll() {
    for (String table : SCHEDULER_TABLES) {
      entityManager.createNativeQuery("DELETE FROM " + table).executeUpdate();
    }
    entityManager.clear();
  }
}
