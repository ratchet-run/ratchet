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
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import run.ratchet.api.BatchPartition;
import run.ratchet.api.JobPage;
import run.ratchet.api.JobProgress;
import run.ratchet.api.JobStatus;
import run.ratchet.api.KeyRange;
import run.ratchet.api.PartitionSetSummary;
import run.ratchet.api.PartitionStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.api.internal.IdempotencyKeyNormalizer;
import run.ratchet.ri.core.internal.JobCheckpointWriter;
import run.ratchet.ri.core.internal.JobProgressWriter;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.CallerPrincipalResolution;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.PartitionEntity;
import run.ratchet.store.entity.PartitionOutcome;
import run.ratchet.store.entity.PartitionSetEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.PartitionStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

/** Seals partition generations and serves their durable metadata. */
@ApplicationScoped
public class PartitionService {
  private static final Logger log = Logger.getLogger(PartitionService.class);
  private final PartitionStore store;
  private final JobCrudStore jobs;
  private final JobStateStore states;
  private final BatchStore batches;
  private final ErrorSanitizer sanitizer;
  private final PayloadSerializer serializer;
  private final Clock clock;
  private final RatchetOptions options;
  private final BatchCompletionTransaction transactions;
  private final JobAuthorizationPolicy policy;
  private final CallerPrincipalProvider principalProvider;
  private Consumer<UUID> cancel;
  private Consumer<JobEntity> cancelBatch = parent -> {};

  protected PartitionService() {
    this(null, null, null, null, null, null, null, null, null, null, null);
  }

  @Inject
  public PartitionService(
      Instance<PartitionStore> store,
      JobCrudStore jobs,
      Instance<JobStateStore> states,
      Instance<BatchStore> batches,
      ErrorSanitizer sanitizer,
      PayloadSerializer serializer,
      Clock clock,
      RatchetOptions options,
      BatchCompletionTransaction transactions,
      JobAuthorizationPolicy policy,
      CallerPrincipalProvider principalProvider,
      Instance<DefaultJobSchedulerService> scheduler,
      Instance<BatchService> batchService) {
    this(
        store.isResolvable() ? store.get() : null,
        jobs,
        states.isResolvable() ? states.get() : null,
        batches.isResolvable() ? batches.get() : null,
        sanitizer,
        serializer,
        clock,
        options,
        transactions,
        policy,
        principalProvider);
    cancel = id -> scheduler.get().cancelJobAsSystem(id);
    cancelBatch = parent -> batchService.get().cancelBatch(parent);
  }

  public PartitionService(
      PartitionStore store,
      JobCrudStore jobs,
      JobStateStore states,
      BatchStore batches,
      ErrorSanitizer sanitizer,
      PayloadSerializer serializer,
      Clock clock,
      RatchetOptions options,
      BatchCompletionTransaction transactions,
      JobAuthorizationPolicy policy,
      CallerPrincipalProvider principalProvider) {
    this.store = store;
    this.jobs = jobs;
    this.states = states;
    this.batches = batches;
    this.sanitizer = sanitizer;
    this.serializer = serializer;
    this.clock = clock;
    this.options = options;
    this.transactions = transactions;
    this.policy = policy;
    this.principalProvider = principalProvider;
  }

  public void configureCancellation(Consumer<UUID> cancel) {
    this.cancel = cancel;
  }

  public void configureBatchCancellation(Consumer<JobEntity> cancelBatch) {
    this.cancelBatch = cancelBatch;
  }

  PartitionStore store() {
    return store;
  }

  int maxPartitions() {
    return options.maintenance().batchMaxPartitions();
  }

  private String currentPrincipal() {
    return CallerPrincipalResolution.resolve(options.callerPrincipalResolver(), principalProvider)
        .orElse(null);
  }

  private static String key(String value) {
    String normalized = IdempotencyKeyNormalizer.normalize(value);
    if (normalized == null) throw new IllegalArgumentException("Partition key must not be blank");
    return normalized;
  }

  private static boolean terminal(JobStatus status) {
    return status == JobStatus.SUCCEEDED
        || status == JobStatus.FAILED
        || status == JobStatus.CANCELED;
  }

  public boolean trySeal(PartitionSetEntity set) {
    if (store == null) return false;
    int generation = set.getLatestGeneration();
    if (set.getSealedGeneration() >= generation) return true;
    JobEntity parent = jobs.findById(set.getLatestBatchId()).orElse(null);
    if (parent == null)
      log.warnf("Missing parent %s for partition set %s", set.getLatestBatchId(), set.getSetId());
    else if (!terminal(parent.getStatus())) return false;
    if (store.countUnfinishedBatchChildren(set.getLatestBatchId()) > 0) return false;
    int after = -1;
    while (true) {
      List<PartitionEntity> page =
          store.findGenerationPartitions(set.getSetId(), generation, after, 500);
      if (page.isEmpty()) break;
      transactions.required(
          () -> {
            for (PartitionEntity row : page) {
              JobEntity child =
                  row.getChildJobId() == null
                      ? null
                      : jobs.findById(row.getChildJobId()).orElse(null);
              if (child == null) {
                row.setOutcome(PartitionOutcome.ABANDONED);
                row.setLastError(null);
              } else {
                row.setOutcome(PartitionOutcome.valueOf(child.getStatus().name()));
                row.setLastError(error(child));
                if (parent != null) {
                  if (child.getStatus() == JobStatus.SUCCEEDED) row.setCheckpoint(null);
                  else if (states != null) {
                    states
                        .getState(child.getId(), JobCheckpointWriter.NAMESPACE)
                        .ifPresent(
                            state ->
                                row.setCheckpoint(
                                    PayloadEncryptor.encryptJsonColumn(
                                        state.json(),
                                        EncryptionHolder.encryptionActiveFor(
                                            parent.isEncryptedPayload()),
                                        EncryptionTarget.partitionCheckpoint(
                                            row.getSetId(), row.getPartitionIndex()))));
                  }
                }
              }
            }
            store.sealPartitions(set.getSetId(), generation, page);
            return null;
          });
      after = page.get(page.size() - 1).getPartitionIndex();
    }
    transactions.required(() -> store.markSealed(set.getSetId(), generation));
    return true;
  }

  public int sweep() {
    if (store == null) return 0;
    int sealed = 0;
    UUID afterSetId = null;
    while (true) {
      List<PartitionSetEntity> page = store.findUnsealedSets(afterSetId, 100);
      if (page.isEmpty()) break;
      for (PartitionSetEntity set : page) {
        try {
          JobEntity parent = jobs.findById(set.getLatestBatchId()).orElse(null);
          if (parent != null && parent.getStatus() == JobStatus.CANCELED) {
            cancelBatch.accept(parent);
          }
          if (trySeal(set)) {
            sealed++;
            continue;
          }
          if (parent == null || terminal(parent.getStatus())) continue;
          var batch = batches.findBatchById(parent.getId());
          if (batch.isPresent()
              && store.countBatchChildren(parent.getId()) < batch.get().getTotalItems()
              && set.getGenerationStartedAt()
                  .isBefore(clock.instant().minus(options.maintenance().batchPartitionGrace()))) {
            cancel.accept(parent.getId());
            log.warnf(
                "Canceled incomplete partition generation %s for set %s",
                parent.getId(), set.getSetId());
          }
        } catch (Exception failure) {
          log.warnf(failure, "Could not recover partition set %s", set.getSetId());
        }
      }
      afterSetId = page.get(page.size() - 1).getSetId();
    }
    return sealed;
  }

  /** Cancels a partially persisted generation using the configured system boundary. */
  public void cancelGeneration(UUID parentId) {
    cancel.accept(parentId);
  }

  /** The authorization policy may receive an archived latestBatchId. */
  public Optional<PartitionSetSummary> getPartitionSet(String partitionKey) {
    if (store == null) return Optional.empty();
    try {
      return store
          .findPartitionSet(key(partitionKey))
          .map(
              set -> {
                if (policy != null) {
                  policy.checkRead(set.getLatestBatchId(), currentPrincipal());
                }
                return new PartitionSetSummary(
                    set.getSetKey(),
                    set.getName(),
                    set.getPartitionCount(),
                    set.getLatestGeneration(),
                    set.getLatestBatchId(),
                    set.getSealedGeneration() < set.getLatestGeneration());
              });
    } catch (JobAuthorizationException denied) {
      return Optional.empty();
    }
  }

  public JobPage<BatchPartition> getBatchPartitions(String partitionKey, int limit, int offset) {
    if (limit < 1 || limit > 1000 || offset < 0)
      throw new IllegalArgumentException("Invalid page request");
    Optional<PartitionSetEntity> found =
        store == null ? Optional.empty() : store.findPartitionSet(key(partitionKey));
    if (found.isEmpty()) return new JobPage<>(List.of(), 0, limit, offset, false, null);
    var set = found.get();
    try {
      if (policy != null) {
        policy.checkRead(set.getLatestBatchId(), currentPrincipal());
      }
    } catch (JobAuthorizationException denied) {
      return new JobPage<>(List.of(), 0, limit, offset, false, null);
    }
    List<BatchPartition> result = new ArrayList<>();
    for (var row : store.findPartitions(set.getSetId(), offset - 1, limit)) {
      JobEntity child =
          row.getChildJobId() == null ? null : jobs.findById(row.getChildJobId()).orElse(null);
      PartitionStatus status =
          row.getOutcome() != null
              ? PartitionStatus.valueOf(row.getOutcome().name())
              : child == null
                  ? PartitionStatus.PENDING
                  : PartitionStatus.valueOf(child.getStatus().name());
      result.add(
          new BatchPartition(
              row.getPartitionIndex(),
              new KeyRange(row.getRangeFrom(), row.getRangeTo()),
              row.getGeneration(),
              row.getChildJobId(),
              status,
              child == null ? null : progress(child.getId()),
              row.getOutcome() != null ? row.getLastError() : child == null ? null : error(child)));
    }
    return new JobPage<>(
        result,
        set.getPartitionCount(),
        limit,
        offset,
        (long) offset + result.size() < set.getPartitionCount(),
        null);
  }

  /** The authorization policy may receive an archived latestBatchId. */
  public boolean deletePartitionSet(String partitionKey) {
    if (store == null) return false;
    var set = store.findPartitionSet(key(partitionKey));
    if (set.isEmpty()) return false;
    if (policy != null)
      policy.checkCancel(
          set.get().getLatestBatchId(), set.get().getOwnerPrincipal(), currentPrincipal());
    if (set.get().getSealedGeneration() < set.get().getLatestGeneration()) {
      trySeal(set.get());
      set = store.findPartitionSet(key(partitionKey));
      if (set.isEmpty()) return false;
    }
    if (set.get().getSealedGeneration() < set.get().getLatestGeneration()
        || !store.deletePartitionSet(set.get().getSetId()))
      throw new IllegalStateException("Partition generation is not sealed");
    return true;
  }

  private String error(JobEntity job) {
    return job.getLastError() == null
        ? null
        : sanitizer.sanitize(new IllegalStateException(job.getLastError()));
  }

  private JobProgress progress(UUID id) {
    if (states == null) return null;
    try {
      var state = states.getState(id, JobProgressWriter.NAMESPACE);
      if (state.isEmpty()) return null;
      Map<?, ?> value = serializer.deserialize(state.get().json(), Map.class);
      long done = new BigDecimal(value.get("done").toString()).longValueExact();
      long total = new BigDecimal(value.get("total").toString()).longValueExact();
      return new JobProgress(done, total, Instant.parse((String) value.get("reportedAt")));
    } catch (Exception failure) {
      log.warnf(failure, "Could not read progress for partition child %s", id);
      return null;
    }
  }
}
