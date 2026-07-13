/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.SerializedJobResult;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

/** Default JSON result persistence with a configurable size cap. */
@ApplicationScoped
public class DefaultResultPersistenceStrategy implements ResultPersistenceStrategy {

  private static final Logger log = Logger.getLogger(DefaultResultPersistenceStrategy.class);

  private final RatchetOptions options;
  private final PayloadSerializer payloadSerializer;
  private final JobCrudStore jobCrudStore;

  protected DefaultResultPersistenceStrategy() {
    this.options = null;
    this.payloadSerializer = null;
    this.jobCrudStore = null;
  }

  @Inject
  public DefaultResultPersistenceStrategy(
      RatchetOptions options, PayloadSerializer payloadSerializer, JobCrudStore jobCrudStore) {
    this.options = options;
    this.payloadSerializer = payloadSerializer;
    this.jobCrudStore = jobCrudStore;
  }

  @Override
  public SerializedJobResult serialize(UUID jobId, Object result) {
    if (result == null) {
      return SerializedJobResult.empty();
    }

    try {
      String resultJson = payloadSerializer.serialize(result);
      String resultType = result.getClass().getName();
      boolean truncated = false;
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
        truncated = true;
      }
      // Encrypt last: the size cap above measures plaintext bytes, and job_result is a JSON/JSONB
      // column, so the engine output is wrapped as a valid-JSON string envelope. Result type/state
      // stays cleartext in its separate column: ordinary values name their deserialization target,
      // while truncated values use a reserved non-class sentinel.
      //
      // The result is written after creation, so the per-job opt-in is not in scope here; we read
      // it back from the row (one indexed lookup) so an opted-in job's result is protected even
      // when
      // the global switch is off.
      boolean optedIn =
          jobCrudStore != null
              && jobCrudStore.findById(jobId).map(JobEntity::isEncryptedPayload).orElse(false);
      boolean active = EncryptionHolder.encryptionActiveFor(optedIn);
      resultJson =
          PayloadEncryptor.encryptJsonColumn(
              resultJson, active, EncryptionTarget.rowBound(ProtectedSurface.RESULT, jobId));
      return truncated
          ? SerializedJobResult.truncated(resultJson)
          : new SerializedJobResult(resultJson, resultType);
    } catch (Exception e) {
      log.warnf(e, "Result serialization error for job %s", jobId);
      throw new IllegalStateException("Result serialization failed for job " + jobId, e);
    }
  }
}
