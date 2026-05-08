package run.ratchet.testsuite.util;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;

class PerformanceBaselineTest {

  @Test
  void constructorThrowsWhenBaselineResourceIsUnreadable() {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(new UnreadableBaselineClassLoader(original));
    try {
      assertThrows(
          UncheckedIOException.class, () -> new PerformanceBaseline("mysql", 0.10, "unused"));
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  private static final class UnreadableBaselineClassLoader extends ClassLoader {

    private UnreadableBaselineClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    public InputStream getResourceAsStream(String name) {
      if ("perf-baselines/mysql-baselines.properties".equals(name)) {
        return new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("synthetic read failure");
          }
        };
      }
      return super.getResourceAsStream(name);
    }
  }
}
