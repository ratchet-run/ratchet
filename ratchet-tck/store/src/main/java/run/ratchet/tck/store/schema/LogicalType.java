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
 * Logical column types in the canonical Ratchet schema. Dialect mappers translate each value into
 * the set of dialect-specific types that satisfy it (e.g. {@link #INT64} → MySQL {@code BIGINT
 * UNSIGNED} ∪ PostgreSQL {@code bigint}; {@link #UUID} → PostgreSQL {@code uuid}, MySQL {@code
 * BINARY(16)} per Hibernate's default mapping for {@code java.util.UUID}).
 */
public enum LogicalType {
  INT32,
  INT64,
  UUID,
  TEXT,
  CHAR_1,
  TIMESTAMP_TZ,
  BOOLEAN,
  JSON
}
