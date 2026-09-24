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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Query;
import jakarta.persistence.TemporalType;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class RatchetJpaPortabilityTest {
  @ParameterizedTest
  @EnumSource(
      value = SqlStoreVendor.class,
      names = {"ORACLE", "SQLSERVER"})
  void verifiesActualTimestampBindingUsingOnlyJpa(SqlStoreVendor vendor) {
    EntityManagerFactory factory = mock(EntityManagerFactory.class);
    EntityManager manager = mock(EntityManager.class);
    Query query = mock(Query.class);
    when(factory.createEntityManager()).thenReturn(manager);
    when(manager.createNativeQuery(anyString())).thenReturn(query);
    DateTimeFormatter format =
        DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    when(query.setParameter(eq(1), any(Timestamp.class), eq(TemporalType.TIMESTAMP)))
        .thenAnswer(
            invocation -> {
              Timestamp value = invocation.getArgument(1);
              when(query.getSingleResult()).thenReturn(format.format(value.toInstant()));
              return query;
            });

    RatchetJpaPrerequisites.validateEntityManagerFactory(factory, vendor);

    verify(factory).createEntityManager();
    verifyNoMoreInteractions(factory);
    verify(query, times(3)).getSingleResult();
    verify(manager).close();
    verify(manager, never()).getTransaction();
  }

  @ParameterizedTest
  @EnumSource(
      value = SqlStoreVendor.class,
      names = {"ORACLE", "SQLSERVER"})
  void closesTheProbeAndPreservesProviderFailures(SqlStoreVendor vendor) {
    EntityManagerFactory factory = mock(EntityManagerFactory.class);
    EntityManager manager = mock(EntityManager.class);
    IllegalStateException failure = new IllegalStateException("provider query failure");
    when(factory.createEntityManager()).thenReturn(manager);
    when(manager.createNativeQuery(anyString())).thenThrow(failure);
    assertThatThrownBy(() -> RatchetJpaPrerequisites.validateEntityManagerFactory(factory, vendor))
        .isSameAs(failure);
    verify(manager).close();
    verify(manager, never()).getTransaction();
  }
}
