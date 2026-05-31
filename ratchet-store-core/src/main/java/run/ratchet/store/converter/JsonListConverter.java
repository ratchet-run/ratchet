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

import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.persistence.Converter;
import java.util.List;

/**
 * JPA {@link AttributeConverter} that converts {@code List<Object>} to/from JSON for database
 * storage.
 *
 * <p>Deserialization intentionally targets raw {@link List} because JSON-B cannot recover erased
 * element types for {@code List<Object>}. Scalar values and nested JSON structures are restored as
 * JSON-B's standard runtime types, not application-specific POJOs.
 */
@Converter
public class JsonListConverter extends AbstractJsonAttributeConverter<List<Object>> {

  public String convertToDatabaseColumn(List<Object> attribute) {
    return super.convertToDatabaseColumn(attribute);
  }

  @Override
  protected String serialize(List<Object> attribute) {
    try (var jsonb = JsonbBuilder.create()) {
      return jsonb.toJson(attribute);
    } catch (JsonbException e) {
      throw e;
    } catch (Exception e) {
      throw new JsonbException("JSON-B list serializer close failed", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<Object> deserialize(String dbData) {
    try (var jsonb = JsonbBuilder.create()) {
      return (List<Object>) jsonb.fromJson(dbData, List.class);
    } catch (JsonbException e) {
      throw e;
    } catch (Exception e) {
      throw new JsonbException("JSON-B list deserializer close failed", e);
    }
  }

  @Override
  protected Class<? extends RuntimeException> conversionExceptionType() {
    return JsonbException.class;
  }

  @Override
  protected String serializationErrorMessage() {
    return "JSON list serialization error";
  }

  @Override
  protected String deserializationErrorMessage() {
    return "JSON list deserialization error";
  }
}
