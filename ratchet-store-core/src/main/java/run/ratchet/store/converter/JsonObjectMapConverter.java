package run.ratchet.store.converter;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import jakarta.persistence.Converter;
import java.util.Map;

/**
 * JPA {@link AttributeConverter} that converts {@code Map<String, Object>} to/from JSON for
 * database storage.
 */
@Converter
public class JsonObjectMapConverter extends AbstractJsonAttributeConverter<Map<String, Object>> {

  public String convertToDatabaseColumn(Map<String, Object> attribute) {
    return super.convertToDatabaseColumn(attribute);
  }

  @Override
  protected String serialize(Map<String, Object> attribute) {
    try (Jsonb jsonb = JsonbBuilder.create()) {
      return jsonb.toJson(attribute);
    } catch (JsonbException e) {
      throw e;
    } catch (Exception e) {
      throw new JsonbException("JSON-B object-map serializer close failed", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Map<String, Object> deserialize(String dbData) {
    try (Jsonb jsonb = JsonbBuilder.create()) {
      return (Map<String, Object>) jsonb.fromJson(dbData, Map.class);
    } catch (JsonbException e) {
      throw e;
    } catch (Exception e) {
      throw new JsonbException("JSON-B object-map deserializer close failed", e);
    }
  }

  @Override
  protected Class<? extends RuntimeException> conversionExceptionType() {
    return JsonbException.class;
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
