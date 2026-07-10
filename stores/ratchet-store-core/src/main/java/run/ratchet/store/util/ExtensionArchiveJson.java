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

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Serializes a job's extension properties and extension-state rows into the denormalized JSON
 * carried on the archive row ({@code scheduler_job_archive.properties} / {@code .extension_state}).
 *
 * <p>The archive copy preserves the rows as stored: extension-state blobs that were encrypted at
 * rest stay ciphertext (the frame remains bound to the original job id and namespace, so it can be
 * decrypted later against {@code original_job_id}), and the {@code encrypted_state} / {@code
 * encryption_key_id} metadata rides along for key-rotation accounting.
 */
public final class ExtensionArchiveJson {

  private ExtensionArchiveJson() {}

  /** One extension-state row as stored, copied verbatim into the archive JSON. */
  public record StateRow(
      String namespace,
      String state,
      boolean encryptedState,
      String encryptionKeyId,
      int version,
      Instant updatedAt) {}

  /**
   * Serializes the property map to a JSON object, or {@code null} when the job has no properties
   * (the archive column stays NULL rather than holding {@code {}}).
   */
  public static String propertiesJson(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return null;
    }
    JsonObjectBuilder builder = Json.createObjectBuilder();
    properties.forEach(builder::add);
    return builder.build().toString();
  }

  /**
   * Serializes the extension-state rows to a JSON array, or {@code null} when the job has none (the
   * archive column stays NULL rather than holding {@code []}).
   */
  public static String extensionStateJson(List<StateRow> rows) {
    if (rows == null || rows.isEmpty()) {
      return null;
    }
    JsonArrayBuilder array = Json.createArrayBuilder();
    for (StateRow row : rows) {
      JsonObjectBuilder entry =
          Json.createObjectBuilder()
              .add("namespace", row.namespace())
              .add("state", row.state())
              .add("encrypted_state", row.encryptedState())
              .add("version", row.version());
      if (row.encryptionKeyId() != null) {
        entry.add("encryption_key_id", row.encryptionKeyId());
      }
      if (row.updatedAt() != null) {
        entry.add("updated_at", row.updatedAt().toString());
      }
      array.add(entry);
    }
    return array.build().toString();
  }
}
