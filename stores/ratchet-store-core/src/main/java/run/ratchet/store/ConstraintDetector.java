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
package run.ratchet.store;

import run.ratchet.api.Incubating;
import run.ratchet.api.Nullable;

/**
 * Detects database constraint violations in a vendor-neutral way.
 *
 * <p>Implementations parse database-specific error messages or codes to identify the type of
 * constraint violation that occurred. This enables the scheduler to handle constraint violations
 * (such as duplicate keys from idempotency checks) without coupling to a specific database vendor.
 *
 * @apiNote This is a store-implementor SPI, not an application-facing API. Application code must
 *     never invoke these methods directly; consume them via the {@link
 *     run.ratchet.store.spi.JobStore} composition. Implementations MUST be thread-safe (the
 *     scheduler invokes them from multiple worker threads). Implementations MUST NOT throw from any
 *     method on this interface; on an unrecognized exception, return {@code false} (for boolean
 *     predicates) or {@code null} (for {@link #constraintName(Exception)}). Implementations MUST be
 *     side-effect free.
 */
@Incubating
public interface ConstraintDetector {

  /**
   * Extracts the database-reported constraint name from a vendor exception.
   *
   * @param e exception thrown by the JDBC / JPA layer; never {@code null}
   * @return the constraint name parsed from the vendor error, or {@code null} when the exception
   *     does not encode a recognizable constraint name
   */
  @Nullable String constraintName(Exception e);

  /**
   * Reports whether the exception was raised by a duplicate-key (unique-constraint) violation.
   *
   * @param e exception thrown by the JDBC / JPA layer; never {@code null}
   * @return {@code true} when the exception encodes a duplicate-key violation, {@code false}
   *     otherwise (including unrecognized exceptions)
   */
  boolean isDuplicateKey(Exception e);

  /**
   * Returns true if the exception was raised by a unique-constraint violation on the business key
   * reservation table.
   *
   * <p>SQL stores use {@code scheduler_business_key_reservation} as the authoritative owner table
   * for active business keys. The vendor-specific detectors still provide duplicate-key and
   * constraint-name parsing because those signals differ by database, but this composed
   * business-key check is shared.
   *
   * @param e exception thrown by the JDBC / JPA layer; never {@code null}
   * @return {@code true} when the exception encodes a duplicate-key violation whose constraint name
   *     targets the business-key reservation table, {@code false} otherwise
   */
  default boolean isDuplicateBusinessKey(Exception e) {
    if (!isDuplicateKey(e)) {
      return false;
    }
    String name = constraintName(e);
    return name != null && name.contains("scheduler_business_key_reservation");
  }

  /**
   * Returns true if the exception was raised by a unique-constraint violation on a job's
   * idempotency key.
   *
   * <p>All stores name this constraint with an {@code idempotency} token: the SQL stores use {@code
   * uk_idempotency_key} and the Mongo store uses {@code idx_job_idempotency_key}. The
   * vendor-specific detectors still provide duplicate-key and constraint-name parsing because those
   * signals differ by database, but this composed idempotency-key check is shared.
   *
   * @param e exception thrown by the JDBC / JPA layer; never {@code null}
   * @return {@code true} when the exception encodes a duplicate-key violation whose constraint name
   *     targets a job idempotency key, {@code false} otherwise
   */
  default boolean isDuplicateIdempotencyKey(Exception e) {
    if (!isDuplicateKey(e)) {
      return false;
    }
    String name = constraintName(e);
    return name != null && name.contains("idempotency");
  }

  /**
   * Reports whether the exception was raised by a deadlock detected by the database engine.
   *
   * @param e exception thrown by the JDBC / JPA layer; never {@code null}
   * @return {@code true} when the exception encodes a deadlock-victim signal, {@code false}
   *     otherwise (including unrecognized exceptions)
   */
  boolean isDeadlock(Exception e);

  /**
   * Reports whether the exception is a transient connection failure (network blip, broken socket,
   * connection-pool exhaustion) that is safe for the caller to retry after a backoff.
   *
   * @param e exception thrown by the JDBC / JPA layer; never {@code null}
   * @return {@code true} when the exception encodes a transient connection-level failure, {@code
   *     false} otherwise (including unrecognized exceptions)
   */
  boolean isTransientConnectionFailure(Exception e);
}
