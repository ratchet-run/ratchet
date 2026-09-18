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
import java.util.concurrent.CountDownLatch;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class ConsumerJobs {
  private final EntityManager entityManager;
  private final String blockJobId;

  public ConsumerJobs(
      EntityManager entityManager, @Value("${consumer.block-job-id:}") String blockJobId) {
    this.entityManager = entityManager;
    this.blockJobId = blockJobId;
  }

  @Transactional
  public void complete(String id) {
    var record = entityManager.find(ConsumerRecord.class, id);
    if (record == null)
      throw new IllegalStateException("Application row was not committed before execution: " + id);
    if (id.equals(blockJobId)) blockUntilProcessIsKilled();
    record.setState("executed");
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
