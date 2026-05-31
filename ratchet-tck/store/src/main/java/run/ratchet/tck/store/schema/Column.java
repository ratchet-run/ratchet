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

/** Canonical column declaration. {@code partOfPrimaryKey} is set by the table builder, not here. */
public record Column(String name, LogicalType type, boolean nullable) {
  public static Column required(String name, LogicalType type) {
    return new Column(name, type, false);
  }

  public static Column nullable(String name, LogicalType type) {
    return new Column(name, type, true);
  }
}
