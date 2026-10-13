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

import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.util.UUID;

@Singleton
public class ConsumerHostDataService {
  private final EntityManager entityManager;

  public ConsumerHostDataService(EntityManager entityManager) {
    this.entityManager = entityManager;
  }

  @Transactional
  public void save(UUID id, ConsumerLabel label, Instant occurredAt) {
    entityManager.persist(new ConsumerUuidRecord(id, label, occurredAt));
  }

  @Transactional
  public ConsumerUuidRecord find(UUID id) {
    return entityManager.find(ConsumerUuidRecord.class, id);
  }
}
