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
 * JPA {@link AttributeConverter} that converts {@code Map<String, Object>} to/from JSON for
 * database storage.
 *
 * <p>Routes through {@link PayloadSerializerHolder} so the framework's {@link
 * run.ratchet.spi.PayloadSerializer} SPI is the single JSON boundary. JPA converters are not
 * CDI-managed beans, so the holder's static registration pattern is used instead of field
 * injection.
 */
@Converter
public class JsonObjectMapConverter extends AbstractJsonAttributeConverter<Map<String, Object>> {

  public String convertToDatabaseColumn(Map<String, Object> attribute) {
    return super.convertToDatabaseColumn(attribute);
  }

  @Override
  protected String serialize(Map<String, Object> attribute) {
    return PayloadSerializerHolder.get().serialize(attribute);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Map<String, Object> deserialize(String dbData) {
    return (Map<String, Object>) PayloadSerializerHolder.get().deserialize(dbData, Map.class);
  }

  @Override
  protected Class<? extends RuntimeException> conversionExceptionType() {
    return IllegalArgumentException.class;
  }

  @Override
  protected String serializationErrorMessage() {
    return "JSON object-map serialization error";
  }

  @Override
  protected String deserializationErrorMessage() {
    return "JSON object-map deserialization error";
  }
}
