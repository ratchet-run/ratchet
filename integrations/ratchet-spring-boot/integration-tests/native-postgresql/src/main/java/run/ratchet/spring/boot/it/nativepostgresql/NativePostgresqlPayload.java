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
package run.ratchet.spring.boot.it.nativepostgresql;

import java.util.Objects;

/** Conservative bean-style JSON-B payload persisted and restored by the native scheduler. */
public final class NativePostgresqlPayload {

  private String name;
  private int sequence;

  public NativePostgresqlPayload() {}

  public NativePostgresqlPayload(String name, int sequence) {
    this.name = name;
    this.sequence = sequence;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getSequence() {
    return sequence;
  }

  public void setSequence(int sequence) {
    this.sequence = sequence;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof NativePostgresqlPayload that)) {
      return false;
    }
    return sequence == that.sequence && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, sequence);
  }

  @Override
  public String toString() {
    return "NativePostgresqlPayload[name=" + name + ", sequence=" + sequence + "]";
  }
}
