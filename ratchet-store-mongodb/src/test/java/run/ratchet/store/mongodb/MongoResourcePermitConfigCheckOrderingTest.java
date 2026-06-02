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
package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.id.UuidV7Factory;

/**
 * Mongo-specific regression for the {@code tryAcquirePermit} config-check ordering. A stray permit
 * row for an <em>unconfigured</em> resource must still fail hard with {@link
 * IllegalArgumentException}, matching the SQL stores which lock the resource-limit row first.
 *
 * <p>This corrupt state — a permit that references a resource with no limit row — cannot be reached
 * through the cross-store {@code ResourcePermitStore} SPI (there is no un-configure operation), so
 * the regression lives here as a store-specific test rather than in the shared TCK contract. The
 * Mongo store previously short-circuited on the existing-permit fast path and returned {@code
 * true}, silently bypassing the documented {@code @throws IllegalArgumentException}.
 */
class MongoResourcePermitConfigCheckOrderingTest {

  private static final MongoTestFixture fixture = new MongoTestFixture();

  @AfterAll
  static void closeFixture() {
    fixture.close();
  }

  @BeforeEach
  @AfterEach
  void cleanup() {
    fixture.cleanupStore();
  }

  @Test
  void tryAcquirePermit_strayPermitForUnconfiguredResource_stillFailsHard() {
    JobEntity job = fixture.store().save(fixture.newPendingJob());
    String resource = "never-configured";

    // Insert a stray permit directly: no SPI call can create a permit for an unconfigured resource.
    ResourcePermitEntity stray = ResourcePermitEntity.create(resource, job.getId(), "node-1");
    stray.setId(UuidV7Factory.create());
    fixture
        .database()
        .getCollection("scheduler_resource_permit")
        .insertOne(DocumentMapper.toDocument(stray));

    assertThrows(
        IllegalArgumentException.class,
        () -> fixture.store().tryAcquirePermit(resource, job.getId(), "node-1"),
        "an unconfigured resource must fail hard even when a stray permit row already exists");
  }
}
