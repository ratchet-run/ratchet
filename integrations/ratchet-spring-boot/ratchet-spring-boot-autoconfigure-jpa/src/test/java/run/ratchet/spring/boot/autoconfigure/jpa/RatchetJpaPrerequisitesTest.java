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
package run.ratchet.spring.boot.autoconfigure.jpa;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Query;
import jakarta.persistence.TemporalType;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

class RatchetJpaPrerequisitesTest {
  @Test
  void utcConfiguredTimezoneIsAcceptedEvenWithNonUtcJvmDefault() {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      for (TimeZone timezone :
          new TimeZone[] {
            TimeZone.getTimeZone("UTC"),
            TimeZone.getTimeZone("GMT"),
            new SimpleTimeZone(0, "application-utc")
          }) {
        RatchetJpaPrerequisites.validateEntityManagerFactory(
            factory(timezone), SqlStoreVendor.ORACLE);
        RatchetJpaPrerequisites.validateEntityManagerFactory(
            factory(timezone), SqlStoreVendor.SQLSERVER);
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void nonUtcExplicitTimezoneIsRejected() {
    for (TimeZone timezone :
        new TimeZone[] {
          TimeZone.getTimeZone("America/Los_Angeles"),
          TimeZone.getTimeZone("Africa/Abidjan"),
          new SimpleTimeZone(3_600_000, "application-offset")
        }) {
      assertThatThrownBy(
              () ->
                  RatchetJpaPrerequisites.validateEntityManagerFactory(
                      factory(timezone), SqlStoreVendor.ORACLE))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("requires UTC JDBC timestamp binding")
          .hasMessageContaining("Configure UTC JDBC timestamp binding");
    }
  }

  @Test
  void absentSettingUsesJvmDefaultAndRequiresUtc() {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      RatchetJpaPrerequisites.validateEntityManagerFactory(factory(null), SqlStoreVendor.SQLSERVER);
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      assertThatThrownBy(
              () ->
                  RatchetJpaPrerequisites.validateEntityManagerFactory(
                      factory(null), SqlStoreVendor.SQLSERVER))
          .hasMessageContaining("requires UTC JDBC timestamp binding");
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void unrelatedVendorsKeepTheirApplicationSettings() {
    EntityManagerFactory factory = mock(EntityManagerFactory.class);
    RatchetJpaPrerequisites.validateEntityManagerFactory(factory, SqlStoreVendor.POSTGRESQL);
    RatchetJpaPrerequisites.validateEntityManagerFactory(factory, SqlStoreVendor.MYSQL);
    verifyNoInteractions(factory);
  }

  private static EntityManagerFactory factory(TimeZone timezone) {
    EntityManagerFactory factory = mock(EntityManagerFactory.class);
    EntityManager manager = mock(EntityManager.class);
    Query query = mock(Query.class);
    when(factory.createEntityManager()).thenReturn(manager);
    when(manager.createNativeQuery(anyString())).thenReturn(query);
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ROOT);
    format.setTimeZone(timezone == null ? TimeZone.getDefault() : timezone);
    when(query.setParameter(eq(1), any(Timestamp.class), eq(TemporalType.TIMESTAMP)))
        .thenAnswer(
            invocation -> {
              Timestamp value = invocation.getArgument(1);
              when(query.getSingleResult()).thenReturn(format.format(value));
              return query;
            });
    return factory;
  }
}
