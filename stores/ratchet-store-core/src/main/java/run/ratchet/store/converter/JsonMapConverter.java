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

import jakarta.persistence.Converter;
import java.util.Map;

/**
 * JPA {@link AttributeConverter} that converts {@code Map<String, String>} to/from JSON for
 * database storage.
 */
@Converter
public class JsonMapConverter extends AbstractJsonAttributeConverter<Map<String, String>> {

  public String convertToDatabaseColumn(Map<String, String> attribute) {
    return super.convertToDatabaseColumn(attribute);
  }

  @Override
  protected String serialize(Map<String, String> attribute) {
    return PayloadSerializerHolder.get().serialize(attribute);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Map<String, String> deserialize(String dbData) {
    Map<?, ?> raw = PayloadSerializerHolder.get().deserialize(dbData, Map.class);
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

  @Override
  protected Class<? extends RuntimeException> conversionExceptionType() {
    return IllegalArgumentException.class;
  }

  @Override
  protected String serializationErrorMessage() {
    return "JSON map serialization error";
  }

  @Override
  protected String deserializationErrorMessage() {
    return "JSON map deserialization error";
  }
}
