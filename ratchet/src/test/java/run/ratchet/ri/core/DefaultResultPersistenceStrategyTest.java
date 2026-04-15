package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.ri.config.DefaultRatchetConfig;
import run.ratchet.ri.config.EnvironmentRatchetConfigSource;
import run.ratchet.spi.SerializedJobResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DefaultResultPersistenceStrategyTest {

  @AfterEach
  void clearProperties() {
    System.clearProperty("ratchet.jobs.max-result-bytes");
    System.clearProperty("RATCHET_JOB_RESULT_MAX_BYTES");
    System.clearProperty("RATCHET_JOBS_MAX_RESULT_BYTES");
  }

  @Test
  void resultLimitUsesUtf8ByteLength() {
    System.setProperty("RATCHET_JOB_RESULT_MAX_BYTES", "5");
    DefaultResultPersistenceStrategy strategy =
        new DefaultResultPersistenceStrategy(
            new DefaultRatchetConfig(new EnvironmentRatchetConfigSource()));

    SerializedJobResult result = strategy.serialize(42L, "\u00e9\u00e9");

    assertEquals(String.class.getName(), result.type());
    assertTrue(result.json().contains("\"_truncated\":true"));
    assertTrue(result.json().contains("\"_originalSize\":6"));
  }
}
