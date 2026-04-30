package run.ratchet.ri.core;

import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.SerializedJobResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.jboss.logging.Logger;

/** Default JSON result persistence with a configurable size cap. */
@ApplicationScoped
public class DefaultResultPersistenceStrategy implements ResultPersistenceStrategy {

  private static final Logger log = Logger.getLogger(DefaultResultPersistenceStrategy.class);

  private final RatchetOptions options;
  private final PayloadSerializer payloadSerializer;

  protected DefaultResultPersistenceStrategy() {
    this.options = null;
    this.payloadSerializer = null;
  }

  @Inject
  public DefaultResultPersistenceStrategy(
      RatchetOptions options, PayloadSerializer payloadSerializer) {
    this.options = options;
    this.payloadSerializer = payloadSerializer;
  }

  @Override
  public SerializedJobResult serialize(UUID jobId, Object result) {
    if (result == null) {
      return SerializedJobResult.empty();
    }

    try {
      String resultJson = payloadSerializer.serialize(result);
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
