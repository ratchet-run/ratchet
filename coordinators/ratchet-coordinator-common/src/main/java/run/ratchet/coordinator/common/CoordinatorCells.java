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
package run.ratchet.coordinator.common;

import java.util.Optional;

/**
 * Shared per-cell identifier suffixing for the bundled cluster coordinators.
 *
 * <p>Multi-cell deployments sharing one cluster resource (Hazelcast topic, Infinispan cache,
 * PostgreSQL LISTEN/NOTIFY channel) append an optional {@code cellId} to a base identifier to
 * isolate wakeup traffic. Before consolidation the separator had drifted: Hazelcast used {@code
 * "-"} while Infinispan and PostgreSQL used {@code "_"}.
 *
 * <p>The separator is pinned to {@code "_"} (underscore). Rationale: the suffixed value feeds a
 * PostgreSQL unquoted-identifier NOTIFY channel, whose charset is {@code [A-Za-z_][A-Za-z0-9_]*} —
 * a hyphen is not a legal PostgreSQL identifier character, but underscore is. Underscore is also
 * valid as a Hazelcast topic name and an Infinispan cache name, so it is the one separator safe for
 * every consumer. It is additionally the pre-existing majority (Infinispan and PostgreSQL already
 * used it); only Hazelcast changes behavior, from {@code base-cell} to {@code base_cell}.
 */
public final class CoordinatorCells {

  /**
   * Separator placed between the base identifier and the cell id. Pinned to underscore because it
   * is the only separator legal in an unquoted PostgreSQL identifier while remaining valid for the
   * Hazelcast and Infinispan consumers.
   */
  public static final String SEPARATOR = "_";

  private CoordinatorCells() {}

  /**
   * Returns {@code base + SEPARATOR + cellId} when a cell id is present, otherwise {@code base}.
   *
   * @param base the base identifier (topic, cache, or channel name)
   * @param cellId the optional per-cell suffix
   * @return the effective identifier with the cell suffix applied
   */
  public static String suffixed(String base, Optional<String> cellId) {
    return cellId.map(c -> base + SEPARATOR + c).orElse(base);
  }
}
