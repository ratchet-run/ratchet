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
package run.ratchet.store.context;

import jakarta.persistence.EntityManager;
import run.ratchet.spi.MetricsCollector;

/**
 * Shared scaffolding for the JDBC/JPA store dialects (MySQL, PostgreSQL, Oracle, SQL Server).
 *
 * <p>Adds the {@link EntityManager} handle and the trusted native-query scalar helpers on top of
 * {@link AbstractStoreContext}. Both helpers translate transient faults so a deadlock or dropped
 * connection during a scalar query surfaces as a retryable {@code RatchetTransientStoreException}.
 */
public abstract class AbstractSqlStoreContext extends AbstractStoreContext {

  private final EntityManager em;

  protected AbstractSqlStoreContext(
      EntityManager em, MetricsCollector metricsCollector, int priorityBoostIntervalMinutes) {
    super(metricsCollector, priorityBoostIntervalMinutes);
    this.em = em;
  }

  public EntityManager em() {
    return em;
  }

  /**
   * Executes a trusted, package-local SQL count query. Callers must pass hard-coded SQL templates
   * only; runtime values belong in {@code params}.
   */
  // Trusted compile-time SQL template; runtime values are bound via setParameter.
  @SuppressWarnings("SqlSourceToSinkFlow")
  public long countByNative(String sql, Object... params) {
    try {
      var query = em.createNativeQuery(sql);
      for (int i = 0; i < params.length; i++) {
        query.setParameter(i + 1, params[i]);
      }
      return ((Number) query.getSingleResult()).longValue();
    } catch (RuntimeException e) {
      throw translateTransientStoreException("count by native SQL", e);
    }
  }

  /**
   * Executes a trusted, package-local SQL scalar query, returning {@code 0.0} for a SQL NULL.
   * Callers must pass hard-coded SQL templates only; runtime values belong in {@code params}.
   */
  // Trusted compile-time SQL template; runtime values are bound via setParameter.
  @SuppressWarnings("SqlSourceToSinkFlow")
  public double doubleByNativeOrZero(String sql, Object... params) {
    try {
      var query = em.createNativeQuery(sql);
      for (int i = 0; i < params.length; i++) {
        query.setParameter(i + 1, params[i]);
      }
      Object result = query.getSingleResult();
      return result == null ? 0.0 : ((Number) result).doubleValue();
    } catch (RuntimeException e) {
      throw translateTransientStoreException("scalar by native SQL", e);
    }
  }
}
