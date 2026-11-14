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
package run.ratchet.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import run.ratchet.api.BusinessKeyLookup;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.IdempotencyKeyLookup;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.core.PartitionService;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.ri.core.internal.DefaultJobExecutorService;
import run.ratchet.ri.core.internal.DurableTerminalEvents;
import run.ratchet.ri.core.internal.ExecutionTargetRouter;
import run.ratchet.ri.core.internal.JobStateFlusher;
import run.ratchet.ri.core.internal.JobTask;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.PartitionSetEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.PartitionStore;

class RatchetEngineWiringTest {
  @Test
  void partitionSweepCancelsWithoutCallingCustomScheduler() {
    JobSchedulerService custom = mock(JobSchedulerService.class);
    JobStore store = mock(JobStore.class);
    UUID parentId = incompletePartitionGeneration(store);
    when(store.compareAndSwapStatus(parentId, JobStatus.RUNNING, JobStatus.CANCELED, null))
        .thenReturn(true);
    JobAuthorizationPolicy policy = mock(JobAuthorizationPolicy.class);
    doThrow(new JobAuthorizationException(parentId, "cancel", null, "denied"))
        .when(policy)
        .checkCancel(any(), any(), isNull());
    runner(store)
        .withBean(JobAuthorizationPolicy.class, () -> policy)
        .withBean(JobSchedulerService.class, () -> custom)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              context.getBean(PartitionService.class).sweep();
              verify(custom, never()).cancelJob(parentId);
              verify(store)
                  .compareAndSwapStatus(parentId, JobStatus.RUNNING, JobStatus.CANCELED, null);
            });
  }

  private UUID incompletePartitionGeneration(JobStore store) {
    PartitionStore partitions = mock(PartitionStore.class);
    BatchStore batches = mock(BatchStore.class);
    when(store.capability(PartitionStore.class)).thenReturn(Optional.of(partitions));
    when(store.capability(BatchStore.class)).thenReturn(Optional.of(batches));
    PartitionSetEntity set = new PartitionSetEntity();
    set.setSetId(UUID.randomUUID());
    set.setLatestBatchId(UUID.randomUUID());
    set.setLatestGeneration(1);
    set.setGenerationStartedAt(Instant.now().minus(Duration.ofHours(1)));
    JobEntity parent = new JobEntity();
    parent.setId(set.getLatestBatchId());
    parent.setStatus(JobStatus.RUNNING);
    BatchEntity batch = new BatchEntity();
    batch.setTotalItems(2);
    when(store.findById(parent.getId())).thenReturn(Optional.of(parent));
    when(batches.findBatchById(parent.getId())).thenReturn(Optional.of(batch));
    when(partitions.findUnsealedSets(null, 100)).thenReturn(List.of(set));
    when(partitions.findUnsealedSets(set.getSetId(), 100)).thenReturn(List.of());
    return parent.getId();
  }

  @Test
  void partitionOptionsAndOptionalCapabilityBindThroughEngine() {
    runner()
        .withPropertyValues("ratchet.batch.max-partitions=23", "ratchet.batch.partition-grace=PT2M")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              var options = context.getBean(RatchetOptions.class);
              assertThat(options.maintenance().batchMaxPartitions()).isEqualTo(23);
              assertThat(options.maintenance().batchPartitionGrace())
                  .isEqualTo(Duration.ofMinutes(2));
              assertThat(context).hasSingleBean(PartitionService.class);
              assertThat(context.getBean(PartitionService.class).getPartitionSet("unknown"))
                  .isEmpty();
            });
  }

  @Test
  void durableConfigurationAndSharedServiceAreWired() {
    runner()
        .withPropertyValues("ratchet.events.durable=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(RatchetOptions.class).events().durable()).isTrue();
              assertThat(context.getBean(RatchetProperties.class).getEvents().isDurable()).isTrue();
              assertThat(context).hasSingleBean(DurableTerminalEvents.class);
            });
  }

  @Test
  void schemaAutoMigrationDefaultsToTrueAndHonorsExplicitValues() {
    runner()
        .run(
            context ->
                assertThat(context.getBean(RatchetOptions.class).schema().autoMigrate()).isTrue());
    for (boolean enabled : new boolean[] {false, true}) {
      runner()
          .withPropertyValues("ratchet.schema.auto-migrate=" + enabled)
          .run(
              context ->
                  assertThat(context.getBean(RatchetOptions.class).schema().autoMigrate())
                      .isEqualTo(enabled));
    }
  }

  @Test
  void customSchedulerPreservesInternalRecurringMaintenance() {
    JobSchedulerService custom = mock(JobSchedulerService.class);
    runner()
        .withBean(JobSchedulerService.class, () -> custom)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(JobSchedulerService.class)).isSameAs(custom);
              assertThat(context).hasSingleBean(RecurringAnnotationMaintenanceService.class);
              assertThat(context).hasSingleBean(SpringRecurringDiscovery.class);
            });
  }

  @Test
  void explicitEngineGraphResolvesWithOnlyMandatoryStoreCapabilities() {
    runner()
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(JobSchedulerService.class);
              assertThat(context).hasSingleBean(BusinessKeyLookup.class);
              assertThat(context).hasSingleBean(IdempotencyKeyLookup.class);
              assertThat(context).hasSingleBean(JobStateFlusher.class);
              assertThat(context).hasSingleBean(SpringRecurringDiscovery.class);
              assertThat(context.getBean(PoolRegistry.class).hasPool(ExecutorTargets.VIRTUAL))
                  .isFalse();
            });
  }

  @Test
  void checkpointIntervalBindsThroughEngineOptions() {
    runner()
        .withPropertyValues("ratchet.execution.checkpoint-flush-interval-ms=250")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(
                      context.getBean(RatchetOptions.class).execution().checkpointFlushIntervalMs())
                  .isEqualTo(250L);
            });
  }

  @Test
  void progressIntervalBindsThroughEngineOptions() {
    runner()
        .withPropertyValues("ratchet.execution.progress-flush-interval-ms=250")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(
                      context.getBean(RatchetOptions.class).execution().progressFlushIntervalMs())
                  .isEqualTo(250L);
            });
  }

  @Test
  void continuationLimitBindsThroughEngineOptions() {
    runner()
        .withPropertyValues("ratchet.execution.max-continuations=250")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(RatchetOptions.class).execution().maxContinuations())
                  .isEqualTo(250);
              // Inspect the task built by the framework's default executor without starting
              // workers.
              var executor = context.getBean(JobExecutorService.class);
              var createTask = DefaultJobExecutorService.class.getDeclaredMethod("createTask");
              createTask.setAccessible(true);
              var task = createTask.invoke(executor);
              var limit = JobTask.class.getDeclaredField("maxContinuations");
              limit.setAccessible(true);
              assertThat(limit.getInt(task)).isEqualTo(250);
            });
  }

  @Test
  void managedExecutorNameDoesNotEnableSpringVirtualPool() {
    runner()
        .withPropertyValues(
            "ratchet.worker.virtual-executor-jndi=java:app/concurrent/VirtualExecutor",
            "spring.threads.virtual.enabled=false",
            "ratchet.worker.default-threading-mode=virtual")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(PoolRegistry.class).hasPool(ExecutorTargets.VIRTUAL))
                  .isFalse();
              var router = context.getBean(ExecutionTargetRouter.class);
              assertThat(router.resolve(null)).isEqualTo(ExecutorTargets.PLATFORM);
              assertThat(router.resolve(ExecutorTargets.VIRTUAL))
                  .isEqualTo(ExecutorTargets.PLATFORM);
            });
  }

  @Test
  void followsBootVirtualThreadSettingOnEachSupportedJavaRuntime() {
    boolean virtual = Runtime.version().feature() >= 21;
    String defaultTarget = virtual ? ExecutorTargets.VIRTUAL : ExecutorTargets.PLATFORM;
    runner()
        .withPropertyValues("spring.threads.virtual.enabled=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(RatchetOptions.class).execution().virtualExecutorJndi())
                  .isNull();
              assertThat(context.getBean(PoolRegistry.class).hasPool(ExecutorTargets.VIRTUAL))
                  .isEqualTo(virtual);
              var router = context.getBean(ExecutionTargetRouter.class);
              assertThat(router.resolve(null)).isEqualTo(defaultTarget);
              assertThat(router.resolve(ExecutorTargets.VIRTUAL)).isEqualTo(defaultTarget);
              assertThat(router.resolve(ExecutorTargets.PLATFORM))
                  .isEqualTo(ExecutorTargets.PLATFORM);
            });
  }

  @Test
  void explicitRatchetDefaultTakesPrecedenceOverBootDefault() {
    runner()
        .withPropertyValues(
            "spring.threads.virtual.enabled=true", "ratchet.worker.default-threading-mode=platform")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              var router = context.getBean(ExecutionTargetRouter.class);
              assertThat(router.resolve(null)).isEqualTo(ExecutorTargets.PLATFORM);
              assertThat(router.resolve(ExecutorTargets.VIRTUAL))
                  .isEqualTo(
                      Runtime.version().feature() >= 21
                          ? ExecutorTargets.VIRTUAL
                          : ExecutorTargets.PLATFORM);
            });
  }

  @Test
  void poolConfigurationRetainsCustomExecutorAndLazyResolution() {
    ExecutorProvider custom = mock(ExecutorProvider.class);
    runner()
        .withPropertyValues("spring.threads.virtual.enabled=true")
        .withBean(ExecutorProvider.class, () -> custom)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(ExecutorProvider.class)).isSameAs(custom);
              assertThat(context.getBean(PoolRegistry.class).hasPool(ExecutorTargets.VIRTUAL))
                  .isEqualTo(Runtime.version().feature() >= 21);
              verifyNoInteractions(custom);
            });
  }

  private ApplicationContextRunner runner() {
    return runner(mock(JobStore.class));
  }

  private ApplicationContextRunner runner(JobStore store) {
    return new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RatchetAutoConfiguration.class, RatchetEngineAutoConfiguration.class))
        .withPropertyValues("ratchet.allowed-packages=example.jobs")
        .withBean(JobStore.class, () -> store)
        .withBean(RatchetRuntime.class, () -> mock(RatchetRuntime.class));
  }
}
