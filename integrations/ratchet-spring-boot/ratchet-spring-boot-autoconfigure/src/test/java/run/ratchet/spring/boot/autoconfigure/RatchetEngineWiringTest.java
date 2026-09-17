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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.ri.core.internal.ExecutionTargetRouter;
import run.ratchet.ri.core.internal.PoolRegistry;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.spi.JobStore;

class RatchetEngineWiringTest {
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
              assertThat(context).hasSingleBean(SpringRecurringDiscovery.class);
              assertThat(context.getBean(PoolRegistry.class).hasPool(ExecutorTargets.VIRTUAL))
                  .isFalse();
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
    return new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RatchetAutoConfiguration.class, RatchetEngineAutoConfiguration.class))
        .withPropertyValues("ratchet.allowed-packages=example.jobs")
        .withBean(JobStore.class, () -> mock(JobStore.class))
        .withBean(RatchetRuntime.class, () -> mock(RatchetRuntime.class));
  }
}
