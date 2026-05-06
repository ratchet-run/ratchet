package run.ratchet.testsuite.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import run.ratchet.spi.RatchetConfigSource;

/** Reads test runtime settings packaged into the Arquillian deployment. */
public final class TestRuntimeConfig implements RatchetConfigSource {

  private static final String RESOURCE = "ratchet-testsuite.properties";
  private static final Properties PROPERTIES = load();

  public static String dbType() {
    return get("ratchet.test.db.type")
        .orElseGet(() -> System.getProperty("ratchet.test.db.type", "mysql"));
  }

  public static String mongoUri() {
    return get("ratchet.test.mongo.uri")
        .orElseGet(() -> System.getProperty("ratchet.test.mongo.uri"));
  }

  public static String mongoDatabase() {
    return get("ratchet.test.mongo.database")
        .orElseGet(() -> System.getProperty("ratchet.test.mongo.database", "ratchet_test"));
  }

  private static Optional<String> get(String key) {
    if (key == null || key.isBlank()) {
      return Optional.empty();
    }
    String value = PROPERTIES.getProperty(key);
    if (value == null || value.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(value);
  }

  private static Properties load() {
    Properties properties = new Properties();
    try (InputStream input =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE)) {
      if (input != null) {
        properties.load(input);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to load " + RESOURCE, e);
    }
    return properties;
  }

  @Override
  public Optional<String> get(String propertyName, String environmentVariable) {
    return get(propertyName).or(() -> get(environmentVariable));
  }
}
