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
package run.ratchet.store.util;

import jakarta.persistence.Query;
import run.ratchet.api.Nullable;

/** Binds the optional owner fence to a native mutation. */
public final class ClaimFence {
  private ClaimFence() {}

  public static Query bind(Query query, @Nullable Long expectedClaimSeq, int parameter) {
    if (expectedClaimSeq != null) query.setParameter(parameter, expectedClaimSeq);
    return query;
  }
}
