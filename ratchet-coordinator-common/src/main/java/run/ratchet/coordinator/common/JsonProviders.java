package run.ratchet.coordinator.common;

import jakarta.json.JsonException;
import jakarta.json.spi.JsonProvider;

/** JSON-P provider probe shared by coordinator transports. */
public final class JsonProviders {

  private JsonProviders() {}

  public static void requireJsonProvider() {
    try {
      JsonProvider.provider();
    } catch (JsonException ex) {
      throw new IllegalStateException(
          "No JSON-P provider (jakarta.json.spi.JsonProvider) found on the classpath. Add"
              + " org.eclipse.parsson:parsson (or another JSON-P 2.x implementation) at runtime"
              + " scope, or deploy into a Jakarta EE container that supplies one.",
          ex);
    }
  }
}
