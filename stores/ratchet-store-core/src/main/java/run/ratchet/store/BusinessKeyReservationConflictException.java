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

import java.io.Serial;
import run.ratchet.api.exception.RatchetTransientStoreException;

/**
 * Store-to-runtime signal that an insert lost the unique active business-key reservation race.
 *
 * <p>Stores must use this only for a duplicate on the business-key reservation, never for
 * deadlocks, connection failures, or unrelated unique constraints. The runtime translates it to the
 * public submission exception once it can determine the winner's idempotency key. The failed
 * transaction may already be rollback-only.
 */
public final class BusinessKeyReservationConflictException extends RatchetTransientStoreException {

  @Serial private static final long serialVersionUID = 1L;

  /**
   * Creates a precise reservation-conflict signal while retaining the underlying store failure.
   *
   * @param businessKey the business key whose active reservation collided
   * @param cause the duplicate-key store failure
   */
  public BusinessKeyReservationConflictException(String businessKey, Throwable cause) {
    super("Active business key in use: " + businessKey, cause);
  }
}
