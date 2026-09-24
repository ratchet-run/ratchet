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

import java.util.concurrent.CountDownLatch;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

@Component
public class MongoConsumerJobs {

  private final MongoTemplate mongoTemplate;
  private final String blockJobId;

  public MongoConsumerJobs(
      MongoTemplate mongoTemplate, @Value("${consumer.block-job-id:}") String blockJobId) {
    this.mongoTemplate = mongoTemplate;
    this.blockJobId = blockJobId;
  }

  public void complete(String id) {
    ConsumerRecord record = mongoTemplate.findById(id, ConsumerRecord.class);
    if (record == null) {
      throw new IllegalStateException(
          "Application document was not persisted before execution: " + id);
    }
    if (id.equals(blockJobId)) blockUntilProcessIsKilled();
    mongoTemplate.save(new ConsumerRecord(id, "executed"));
  }

  private static void blockUntilProcessIsKilled() {
    try {
      new CountDownLatch(1).await();
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Blocked consumer job was interrupted", failure);
    }
  }
}
