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
package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Serialized representation of a completed job result for persistence.
 *
 * <p>The {@code type} normally names the result value's Java class. The reserved {@link
 * #TRUNCATED_RESULT_TYPE} value instead identifies a truncation marker that must not be
 * deserialized as a user result.
 */
@Incubating
public record SerializedJobResult(String json, String type) {

  /** Reserved non-class type used when the persisted JSON is truncation metadata. */
  public static final String TRUNCATED_RESULT_TYPE = "@ratchet:truncated-result:v1";

  /**
   * Returns an empty serialized result.
   *
   * <p>Both {@code json} and {@code type} are {@code null}, which means no result value should be
   * persisted for the job.
   */
  public static SerializedJobResult empty() {
    return new SerializedJobResult(null, null);
  }

  /**
   * Returns a serialized truncation marker.
   *
   * <p>The marker JSON may be encrypted before this value reaches the store. The reserved type
   * remains visible so readers can reject value-based operations without inspecting or
   * deserializing the marker payload.
   *
   * @param markerJson JSON metadata describing the truncated result
   * @return serialized truncation marker
   */
  public static SerializedJobResult truncated(String markerJson) {
    return new SerializedJobResult(markerJson, TRUNCATED_RESULT_TYPE);
  }

  /** Returns whether this representation contains truncation metadata instead of a result value. */
  public boolean isTruncated() {
    return isTruncatedType(type);
  }

  /** Returns whether {@code type} is Ratchet's reserved truncation-state sentinel. */
  public static boolean isTruncatedType(String type) {
    return TRUNCATED_RESULT_TYPE.equals(type);
  }
}
