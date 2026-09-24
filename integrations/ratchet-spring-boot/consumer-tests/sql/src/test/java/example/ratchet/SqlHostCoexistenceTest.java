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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import run.ratchet.consumer.sql.SqlDatabase;

/** Proves Ratchet's persistence mappings do not alter application-owned Hibernate mappings. */
class SqlHostCoexistenceTest {
  private static SqlDatabase database;
  private static TimeZone originalTimeZone;

  @BeforeAll
  static void startDatabaseOutsideUtc() {
    originalTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    database = SqlDatabase.start();
  }

  @AfterAll
  static void stopDatabaseAndRestoreTimeZone() {
    try {
      if (database != null) database.close();
    } finally {
      if (originalTimeZone != null) TimeZone.setDefault(originalTimeZone);
    }
  }

  @Test
  void applicationUuidConverterAndInstantRoundTripWithoutGlobalTypeOverrides() {
    UUID id = UUID.randomUUID();
    ConsumerLabel label = new ConsumerLabel("application-owned converter");
    Instant occurredAt = Instant.parse("2026-03-08T18:24:36.123456Z");

    try (ConfigurableApplicationContext context = application()) {
      ConsumerHostDataService service = context.getBean(ConsumerHostDataService.class);
      service.save(id, label, occurredAt);

      ConsumerUuidRecord stored = service.find(id);
      assertThat(stored.getId()).isEqualTo(id);
      assertThat(stored.getLabel()).isEqualTo(label);
      assertThat(stored.getOccurredAt()).isEqualTo(occurredAt);

      if (database.store().equals("sqlserver")) {
        JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
        assertThat(
                jdbc.queryForObject(
                    """
                    select TYPE_NAME(c.user_type_id)
                    from sys.columns c
                    join sys.tables t on t.object_id = c.object_id
                    where t.name = 'consumer_uuid_record' and c.name = 'id'
                    """,
                    String.class))
            .isEqualToIgnoringCase("uniqueidentifier");
      }
    }
  }

  private static ConfigurableApplicationContext application() {
    return new SpringApplicationBuilder(ConsumerApplication.class)
        .properties(database.properties())
        .run();
  }
}
