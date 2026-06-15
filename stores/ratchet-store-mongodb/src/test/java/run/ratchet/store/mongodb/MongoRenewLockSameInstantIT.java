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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Lock expiry is stored at millisecond precision, so two renewals computing the same expires_at
 * leave the row matched but unmodified. renewLock must report success on a matched row it still
 * owns, otherwise a lease holder treating false as "lease lost" abandons a lease it actually holds.
 */
class MongoRenewLockSameInstantIT extends BaseDocumentStoreIT {

  @Test
  void twoRenewalsInTheSameInstantBothSucceed() {
    Instant fixed = Instant.now();
    ((MongoJobStoreImpl) store()).setLockClock(() -> fixed);

    assertTrue(store().tryLock("lease-A", Duration.ofSeconds(30), "node-1"));

    // Same clock + same extension on both renewals yields an identical expires_at: the second
    // update
    // matches the owned lock but modifies nothing.
    assertTrue(
        store().renewLock("lease-A", Duration.ofSeconds(30), "node-1"),
        "first renewal must succeed");
    assertTrue(
        store().renewLock("lease-A", Duration.ofSeconds(30), "node-1"),
        "no-op renewal on an owned lock must still report success");

    assertFalse(
        store().renewLock("lease-A", Duration.ofSeconds(30), "node-2"),
        "renewal by a non-owner must fail");
  }
}
