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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;

/**
 * A BSON Date stores milliseconds, so persisting an Instant with sub-millisecond nanos and reading
 * it back must not surprise the caller. The store truncates the in-memory entity to milliseconds at
 * the write boundary so the saved object equals what a later findById returns.
 */
class MongoInstantPrecisionIT extends BaseDocumentStoreIT {

  @Test
  void saveTruncatesInstantsToMillisSoWriteThenReadIsStable() {
    Instant subMillis = Instant.now().truncatedTo(ChronoUnit.SECONDS).plusNanos(123_456_789L);

    JobEntity job = newPendingJob();
    job.setScheduledTime(subMillis);
    job = store().save(job);

    // The in-memory entity must already reflect what storage can hold.
    assertEquals(subMillis.truncatedTo(ChronoUnit.MILLIS), job.getScheduledTime());

    JobEntity reloaded = store().findById(job.getId()).orElseThrow();
    assertEquals(
        job.getScheduledTime(),
        reloaded.getScheduledTime(),
        "scheduled_time must round-trip without precision drift");
  }
}
