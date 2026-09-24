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
package example.ratchet;

import jakarta.persistence.EntityManager;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;

@Service
public class ConsumerService {
  private final EntityManager entityManager;
  private final JobSchedulerService scheduler;
  private final ConsumerJobs jobs;

  public ConsumerService(
      EntityManager entityManager, JobSchedulerService scheduler, ConsumerJobs jobs) {
    this.entityManager = entityManager;
    this.scheduler = scheduler;
    this.jobs = jobs;
  }

  @Transactional
  public JobHandle submit(String id) {
    entityManager.persist(new ConsumerRecord(id, "submitted"));
    return scheduler.enqueue(() -> jobs.complete(id)).submit();
  }

  @Transactional
  public JobHandle submitBatch(String... ids) {
    for (String id : ids) {
      entityManager.persist(new ConsumerRecord(id, "submitted"));
    }
    return scheduler.enqueueBatch("consumer-batch").forEach(List.of(ids), jobs::complete).submit();
  }
}
