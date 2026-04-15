package run.ratchet.ri.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.spi.RatchetConfig;
import run.ratchet.spi.RatchetConfigKey;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.SerializedJobResult;
import run.ratchet.store.util.ObjectMapperFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import org.jboss.logging.Logger;

/** Default JSON result persistence with a configurable size cap. */
@ApplicationScoped
public class DefaultResultPersistenceStrategy implements ResultPersistenceStrategy {

  private static final Logger log = Logger.getLogger(DefaultResultPersistenceStrategy.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.get();

  static final RatchetConfigKey<Long> RESULT_MAX_BYTES =
      RatchetConfigKey.longAtLeast(
          "ratchet.jobs.max-result-bytes",
          "RATCHET_JOB_RESULT_MAX_BYTES",
          "ratchet.jobs.max-result-bytes",
          "RATCHET_JOBS_MAX_RESULT_BYTES",
          65536L,
          0L);

  private final RatchetConfig config;

  protected DefaultResultPersistenceStrategy() {
    this.config = null;
  }

  @Inject
  public DefaultResultPersistenceStrategy(RatchetConfig config) {
    this.config = config;
  }

  @Override
  public SerializedJobResult serialize(long jobId, Object result) {
    if (result == null) {
      return SerializedJobResult.empty();
    }

    try {
      String resultJson = OBJECT_MAPPER.writeValueAsString(result);
      String resultType = result.getClass().getName();
      long maxBytes = config.get(RESULT_MAX_BYTES);
      int resultBytes = resultJson.getBytes(StandardCharsets.UTF_8).length;
      if (maxBytes > 0 && resultBytes > maxBytes) {
        log.warnf(
            "Job %s result exceeds %s=%s bytes (actual=%s); truncating to marker",
            jobId, RESULT_MAX_BYTES.name(), maxBytes, resultBytes);
        resultJson =
            "{\"_truncated\":true,\"_originalSize\":"
                + resultBytes
                + ",\"_maxAllowed\":"
                + maxBytes
                + ",\"_resultType\":\""
                + resultType.replace("\"", "\\\"")
                + "\"}";
      }
      return new SerializedJobResult(resultJson, resultType);
    } catch (Exception e) {
      log.warnf("Result serialization error for job %s: %s", jobId, e.getMessage());
      return SerializedJobResult.empty();
    }
  }
}
