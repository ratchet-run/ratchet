package run.ratchet.store.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class JobQueryCursorTest {

  @Test
  void decodeWrapsInvalidBase64AsMalformedCursor() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> JobQueryCursor.decode("%%%"));

    assertEquals("Malformed pagination cursor", ex.getMessage());
    assertInstanceOf(IllegalArgumentException.class, ex.getCause());
  }
}
