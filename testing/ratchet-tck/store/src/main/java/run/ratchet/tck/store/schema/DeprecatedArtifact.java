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

/**
 * Schema artifacts that a migration explicitly removed. The conformance contract verifies these are
 * absent from any conforming store, catching upgrade paths that left obsolete columns or indexes
 * behind. {@code sinceVersion} is the schema version that performed the removal.
 */
public sealed interface DeprecatedArtifact {

  int sinceVersion();

  record DroppedTable(String table, int sinceVersion) implements DeprecatedArtifact {}

  record DroppedColumn(String table, String column, int sinceVersion)
      implements DeprecatedArtifact {}

  record DroppedIndex(String table, String index, int sinceVersion) implements DeprecatedArtifact {}
}
