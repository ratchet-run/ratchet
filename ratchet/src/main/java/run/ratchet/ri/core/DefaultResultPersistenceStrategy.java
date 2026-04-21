package run.ratchet.ri.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.config.RatchetOptionsResolver;
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

  private final RatchetOptions options;

  protected DefaultResultPersistenceStrategy() {
    this.options = null;
  }

  @Inject
  public DefaultResultPersistenceStrategy(RatchetOptionsResolver optionsResolver) {
    this(optionsResolver.get());
  }

  public DefaultResultPersistenceStrategy(RatchetOptions options) {
    this.options = options;
  }

  @Override
  public SerializedJobResult serialize(long jobId, Object result) {
    if (result == null) {
      return SerializedJobResult.empty();
    }

    try {
      String resultJson = OBJECT_MAPPER.writeValueAsString(result);
      String resultType = result.getClass().getName();
      long maxBytes = options.payload().maxResultBytes();
      int resultBytes = resultJson.getBytes(StandardCharsets.UTF_8).length;
      if (maxBytes > 0 && resultBytes > maxBytes) {
        log.warnf(
            "Job %s result exceeds configured maxResultBytes=%s bytes (actual=%s); truncating to marker",
            jobId, maxBytes, resultBytes);
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
