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
package example.ratchet.mongo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;

@Service
public class MongoConsumerService {

  private final MongoTemplate mongoTemplate;
  private final JobSchedulerService scheduler;
  private final MongoConsumerJobs jobs;

  public MongoConsumerService(
      MongoTemplate mongoTemplate, JobSchedulerService scheduler, MongoConsumerJobs jobs) {
    this.mongoTemplate = mongoTemplate;
    this.scheduler = scheduler;
    this.jobs = jobs;
  }

  public JobHandle submit(String id) {
    mongoTemplate.save(new ConsumerRecord(id, "submitted"));
    return scheduler.enqueue(() -> jobs.complete(id)).submit();
  }
}
