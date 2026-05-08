package run.ratchet.store.converter;

import jakarta.persistence.AttributeConverter;

abstract class AbstractJsonAttributeConverter<T> implements AttributeConverter<T, String> {

  @Override
  public String convertToDatabaseColumn(T attribute) {
    if (attribute == null) {
      return null;
    }
    try {
      return serialize(attribute);
    } catch (RuntimeException e) {
      throw wrapIfExpected(e, serializationErrorMessage());
    }
  }

  @Override
  public T convertToEntityAttribute(String dbData) {
    if (dbData == null || dbData.isEmpty()) {
      return null;
    }
    try {
      return deserialize(dbData);
    } catch (RuntimeException e) {
      throw wrapIfExpected(e, deserializationErrorMessage());
    }
  }

  protected abstract String serialize(T attribute);

  protected abstract T deserialize(String dbData);

  protected abstract Class<? extends RuntimeException> conversionExceptionType();

  protected abstract String serializationErrorMessage();

  protected abstract String deserializationErrorMessage();

  private IllegalArgumentException wrapIfExpected(RuntimeException e, String message) {
    if (conversionExceptionType().isInstance(e)) {
      return new IllegalArgumentException(message, e);
    }
    throw e;
  }
}
