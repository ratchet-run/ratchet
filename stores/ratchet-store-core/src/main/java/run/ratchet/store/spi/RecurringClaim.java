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
package run.ratchet.store.spi;

import java.util.Objects;
import java.util.UUID;
import run.ratchet.api.Incubating;

/** A claimed master and its opaque lease token; null tokens denote transaction-scoped SQL locks. */
@Incubating
public record RecurringClaim(RecurringJobDefinition definition, UUID token) {
  public RecurringClaim {
    Objects.requireNonNull(definition, "definition");
  }
}
