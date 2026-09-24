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
package run.ratchet.consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.tck.api.*;

/** Runs existing API contracts against each real Boot consumer, without replacing its engine. */
public abstract class SpringApiContracts {
  protected abstract ConfigurableApplicationContext context();

  protected abstract void resetDatabase();

  protected boolean transactionalStore() {
    return true;
  }

  @Test
  void delayedJobWaitsAndThenExecutesUsingTheRealClock() {
    var runtime = runtime();
    runtime.clear();
    try {
      var handle = runtime.scheduler().schedule(Duration.ofSeconds(2), TckJobs::noop).submit();
      runtime.probe().track(handle);
      assertFalse(runtime.probe().awaitExecuted(handle, Duration.ofMillis(300)));
      assertTrue(runtime.probe().awaitCompleted(handle, Duration.ofSeconds(30)));
    } finally {
      runtime.clear();
      TckJobs.resetAll();
    }
  }

  protected RatchetTckRuntime runtime() {
    return new RatchetTckRuntime() {
      public JobSchedulerService scheduler() {
        return context().getBean(JobSchedulerService.class);
      }

      public RatchetTckProbe probe() {
        return context().getBean(ListenerProbe.class);
      }

      public Optional<TestClock> clock() {
        return Optional.empty();
      }

      public OptionalLong maxPayloadBytes() {
        return OptionalLong.of(
            context().getBean(RatchetOptions.class).payload().maxPayloadKb() * 1024L);
      }

      public boolean supportsCallerTransactionRollback() {
        return transactionalStore();
      }

      public void clear() {
        RatchetTckRuntimeSupport.clearRuntime(
            "Spring consumer",
            context().getBean(DrainController.class)::setDraining,
            context().getBean(JobExecutorService.class)::awaitIdle,
            SpringApiContracts.this::resetDatabase,
            context().getBean(ListenerProbe.class)::reset);
        context().getBean(CircuitBreakerRegistry.class).resetBreaker("TckJobs.throwIntentional");
        TckJobs.resetAll();
      }
    };
  }

  @Nested
  class JobLifecycle extends AbstractJobLifecycleContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class JobRetry extends AbstractJobRetryContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class JobCancel extends AbstractJobCancelContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class JobControlReturn extends AbstractJobControlReturnContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class Idempotency extends AbstractIdempotencyContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class BusinessKey extends AbstractBusinessKeyContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class BatchRetry extends AbstractBatchRetryContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class SimpleWorkflow extends AbstractSimpleWorkflowContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class BulkCancelEvent extends AbstractBulkCancelEventContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class BulkRetry extends AbstractBulkRetryContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class BroadcastSignal extends AbstractBroadcastSignalContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class SignalPayload extends AbstractSignalPayloadContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class SignalDecision extends AbstractSignalDecisionContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class PayloadSize extends AbstractPayloadSizeContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class DelayedScheduling extends AbstractDelayedSchedulingContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }
  }

  @Nested
  class JobQuery extends AbstractJobQueryContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }

    @Override
    protected JobQueryService queryService() {
      return context().getBean(JobQueryService.class);
    }
  }

  @Nested
  class ExclusiveWorkflow extends AbstractExclusiveWorkflowContract {
    @Override
    protected RatchetTckRuntime runtime() {
      return SpringApiContracts.this.runtime();
    }

    @Override
    protected JobQueryService queryService() {
      return context().getBean(JobQueryService.class);
    }
  }
}
