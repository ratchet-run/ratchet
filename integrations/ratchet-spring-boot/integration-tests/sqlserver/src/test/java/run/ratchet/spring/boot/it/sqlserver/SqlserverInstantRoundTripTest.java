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
package run.ratchet.spring.boot.it.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spring.boot.it.sqlserver.fixture.ratchetonly.RatchetOnlyApplication;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.sqlserver.SqlserverJobStore;

/**
 * Pins the JVM default zone to a non-UTC offset so that a regression in {@code
 * RowValues.instantOrNull}'s {@code LocalDateTime} handling has teeth on UTC CI runners: reading a
 * SQL Server {@code DATETIME2(6)} column back through Hibernate 7's native-query path must recover
 * the exact instant that was written, not a value shifted by the JVM default zone offset.
 */
class SqlserverInstantRoundTripTest extends SqlserverIntegrationTestSupport {

  // run.ratchet.ri.core.internal.ChainScheduler.CHAIN_LOCK_TIME — inlined rather than imported
  // since that class lives in an internal package not meant to be depended on across modules.
  private static final Instant CHAIN_LOCK_TIME = Instant.parse("9999-12-31T23:59:59Z");

  private TimeZone originalTimeZone;

  @BeforeEach
  void pinNonUtcDefaultZone() {
    // Must run before the Spring context (and its Hibernate session factory) is created inside
    // the @Test method, since that's when connections and the JVM-zone-dependent conversion
    // behavior get established.
    originalTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
  }

  @AfterEach
  void restoreDefaultZone() {
    TimeZone.setDefault(originalTimeZone);
  }

  @Test
  void instantRoundTripsExactlyThroughSqlserverDatetime2Column() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions("sqlserver"))
        .run(
            context -> {
              SqlserverJobStore store = store(context);

              Instant currentInstant = Instant.now().truncatedTo(ChronoUnit.MICROS);

              JobEntity sentinelJob = newPendingJob(UUID.randomUUID().toString());
              sentinelJob.setScheduledTime(CHAIN_LOCK_TIME);
              JobEntity persistedSentinel = store.create(sentinelJob);

              JobEntity currentJob = newPendingJob(UUID.randomUUID().toString());
              currentJob.setScheduledTime(currentInstant);
              JobEntity persistedCurrent = store.create(currentJob);

              JobEntity reloadedSentinel = store.findById(persistedSentinel.getId()).orElseThrow();
              JobEntity reloadedCurrent = store.findById(persistedCurrent.getId()).orElseThrow();

              assertThat(reloadedSentinel.getScheduledTime()).isEqualTo(CHAIN_LOCK_TIME);
              assertThat(reloadedCurrent.getScheduledTime()).isEqualTo(currentInstant);
            });
  }
}
