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
package run.ratchet.store.converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** JSON map column encoding shared by entities and converters without a JPA runtime dependency. */
public final class JsonMapColumns {

  private JsonMapColumns() {}

  public static String writeStringMap(Map<String, String> map) {
    return map == null ? null : PayloadSerializerHolder.get().serialize(map);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, String> readStringMap(String json) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    Map<?, ?> raw = PayloadSerializerHolder.get().deserialize(json, Map.class);
    if (raw == null) {
      return null;
    }
    for (Map.Entry<?, ?> entry : raw.entrySet()) {
      if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
        throw new IllegalArgumentException(
            "JSON map column contains non-String entry: key="
                + entry.getKey()
                + " ("
                + (entry.getKey() == null ? "null" : entry.getKey().getClass().getSimpleName())
                + "), value="
                + entry.getValue()
                + " ("
                + (entry.getValue() == null ? "null" : entry.getValue().getClass().getSimpleName())
                + ")");
      }
    }
    return (Map<String, String>) raw;
  }

  public static String writeObjectMap(Map<String, Object> map) {
    return map == null ? null : PayloadSerializerHolder.get().serialize(map);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> readObjectMap(String json) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    return (Map<String, Object>) PayloadSerializerHolder.get().deserialize(json, Map.class);
  }

  /**
   * Returns a deep, unmodifiable copy of a JSON object map. Nested maps and collections are copied
   * and frozen too, so no change can reach the cached map without going through the setter that
   * rewrites the persisted JSON. Leaf values are kept as they are.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> freezeObjectMap(Map<String, Object> map) {
    return map == null ? null : (Map<String, Object>) freeze(map);
  }

  private static Object freeze(Object value) {
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> copy = new LinkedHashMap<>();
      map.forEach((k, v) -> copy.put(k, freeze(v)));
      return Collections.unmodifiableMap(copy);
    }
    if (value instanceof Collection<?> collection) {
      ArrayList<Object> copy = new ArrayList<>(collection.size());
      collection.forEach(v -> copy.add(freeze(v)));
      return Collections.unmodifiableList(copy);
    }
    return value;
  }
}
