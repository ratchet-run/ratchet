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
package run.ratchet.quarkus.it.tck;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.util.List;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/** PostgreSQL-backed scheduler table cleaner for the Quarkus TCK harness. */
@ApplicationScoped
public class QuarkusTckStoreCleaner {

  private static final List<String> SCHEDULER_TABLES =
      List.of(
          "scheduler_workflow_condition",
          "scheduler_job_log",
          "scheduler_job_execution",
          "scheduler_resource_permit",
          "scheduler_job_tag",
          "scheduler_batch_metrics",
          "scheduler_batch",
          "scheduler_job_archive",
          "scheduler_job_properties",
          "scheduler_job_extension_state",
          "scheduler_business_key_reservation",
          "scheduler_job_queue",
          "scheduler_job",
          "scheduler_recurring_job_archive",
          "scheduler_recurring_job",
          "scheduler_lock",
          "scheduler_resource_limit",
          "scheduler_node");

  @Inject RatchetEntityManagerProvider entityManagerProvider;

  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void truncateAll() {
    EntityManager entityManager = entityManagerProvider.getEntityManager();
    entityManager
        .createNativeQuery(
            "TRUNCATE TABLE "
                + String.join(", ", SCHEDULER_TABLES)
                + " RESTART IDENTITY CASCADE")
        .executeUpdate();
    entityManager.clear();
  }
}
