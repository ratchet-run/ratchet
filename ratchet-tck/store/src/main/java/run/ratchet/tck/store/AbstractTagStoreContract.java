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

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract tests for {@code TagStore} writes.
 *
 * <p>{@code insertTags} and {@code deleteTagsByJobId} are part of the mandatory core surface, so
 * this contract verifies them through {@code deleteTagsByJobId}'s own row count — a core read that
 * exists on every conforming store. It deliberately avoids {@code findJobIdsByTag}, which lives on
 * the optional {@code JobQueryStore} capability: routing the only assertion through that accessor
 * would skip the whole core tag-write contract on a core-only store. Tag-index reads (lookup by
 * tag, pagination, ordering) are covered by the optional query-store contract where the capability
 * gating is correct.
 */
public abstract class AbstractTagStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupTagFixture() {
    cleanupStore();
  }

  @Test
  void insertTags_persistsOneRowPerTag() {
    var saved = persist(newPendingJob());
    store().insertTags(saved.getId(), List.of("tag1", "tag2"));

    assertEquals(
        2,
        store().deleteTagsByJobId(saved.getId()),
        "insertTags must persist one row per tag; the delete count reflects both writes");
  }

  @Test
  void deleteTagsByJobId_removesAllTags() {
    var saved = persist(newPendingJob());
    store().insertTags(saved.getId(), List.of("tag1", "tag2"));

    assertEquals(
        2, store().deleteTagsByJobId(saved.getId()), "deleteTagsByJobId should remove every tag");
    assertEquals(
        0,
        store().deleteTagsByJobId(saved.getId()),
        "a second delete should find nothing left to remove");
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
        1,
        store().deleteTagsByJobId(saved.getId()),
        "Duplicate tag insertion must leave a single association");
  }

  @Test
  void insertTags_emptyList_isNoOp() {
    var saved = persist(newPendingJob());

    assertDoesNotThrow(
        () -> store().insertTags(saved.getId(), List.of()),
        "Inserting empty tag list should not throw");
    assertEquals(
        0, store().deleteTagsByJobId(saved.getId()), "an empty insert must write no tag rows");
  }

  @Test
  void deleteTagsByJobId_unknownJob_returnsZero() {
    int deleted = store().deleteTagsByJobId(new UUID(0L, Long.MAX_VALUE));

    assertEquals(0, deleted, "deleteTagsByJobId for unknown job should return 0");
  }
}
