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
package run.ratchet.tck.store.schema;

import java.util.List;

/**
 * Canonical Ratchet schema, expressed as logical declarations. Tables list required tables (extras
 * are tolerated — the contract verifies presence and conformance, not exclusivity). Deprecated
 * artifacts list what must be absent for the bidirectional check.
 */
public record SchemaSpec(int version, List<Table> tables, List<DeprecatedArtifact> deprecated) {

  public SchemaSpec {
    tables = List.copyOf(tables);
    deprecated = List.copyOf(deprecated);
  }
}
