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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract tests for {@code TagStore} writes.
 *
 * <p>Tag writes are verified through {@code findJobIdsByTag} (a {@code JobQueryStore} read); there
 * is no read on the write-only {@code TagStore} contract itself. Tag id lookup and per-tag
 * aggregate counts have their own contracts on the optional query/analytics capabilities.
 */
public abstract class AbstractTagStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupTagFixture() {
    cleanupStore();
  }

  @Test
  void insertTags_andFindByTag_returnsJobId() {
    var saved = persist(newPendingJob());
    store().insertTags(saved.getId(), List.of("tag1", "tag2"));

    List<UUID> ids = queryStore().findJobIdsByTag("tag1", 10, 0);

    assertTrue(ids.contains(saved.getId()), "findJobIdsByTag should return the tagged job");
    assertTrue(
        queryStore().findJobIdsByTag("tag2", 10, 0).contains(saved.getId()),
        "findJobIdsByTag should return the job for every inserted tag");
  }

  @Test
  void deleteTagsByJobId_removesAllTags() {
    var saved = persist(newPendingJob());
    store().insertTags(saved.getId(), List.of("tag1", "tag2"));

    int deleted = store().deleteTagsByJobId(saved.getId());

    assertTrue(deleted > 0, "deleteTagsByJobId should report removed tags");
    assertTrue(
        queryStore().findJobIdsByTag("tag1", 10, 0).isEmpty(),
        "findJobIdsByTag should return empty after deletion");
    assertTrue(
        queryStore().findJobIdsByTag("tag2", 10, 0).isEmpty(),
        "findJobIdsByTag should return empty after deletion");
  }

  @Test
  void insertTags_duplicateTag_isIdempotent() {
    var saved = persist(newPendingJob());

    assertDoesNotThrow(
        () -> {
          store().insertTags(saved.getId(), List.of("dup-tag", "dup-tag"));
          store().insertTags(saved.getId(), List.of("dup-tag"));
        },
        "Inserting the same tag twice should not throw");

    assertEquals(
        List.of(saved.getId()),
        queryStore().findJobIdsByTag("dup-tag", 10, 0),
        "Duplicate tag insertion must leave a single association");
  }

  @Test
  void insertTags_emptyList_isNoOp() {
    var saved = persist(newPendingJob());

    assertDoesNotThrow(
        () -> store().insertTags(saved.getId(), List.of()),
        "Inserting empty tag list should not throw");
  }

  @Test
  void deleteTagsByJobId_unknownJob_returnsZero() {
    int deleted = store().deleteTagsByJobId(new UUID(0L, Long.MAX_VALUE));

    assertEquals(0, deleted, "deleteTagsByJobId for unknown job should return 0");
  }
}
