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
package run.ratchet.ri.core.internal;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.jboss.logging.Logger;
import run.ratchet.api.JobContext;
import run.ratchet.api.event.JobCallbackFailedEvent;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;

/** Invokes lifecycle callbacks with security validation and guarded failure reporting. */
public final class LifecycleCallbackInvoker {

  private static final Logger log = Logger.getLogger(LifecycleCallbackInvoker.class);
  private final PreExecutionValidator validationFacade;
  private final JobPayloadInvoker payloadInvoker;
  private final PayloadSerializer payloadSerializer;
  private final ExecutionObserver observabilityFacade;
  private final Clock clock;

  public LifecycleCallbackInvoker(
      PreExecutionValidator validationFacade,
      JobPayloadInvoker payloadInvoker,
      PayloadSerializer payloadSerializer,
      ExecutionObserver observabilityFacade,
      Clock clock) {
    this.validationFacade = validationFacade;
    this.payloadInvoker = payloadInvoker;
    this.payloadSerializer = payloadSerializer;
    this.observabilityFacade = observabilityFacade;
    this.clock = clock;
  }

  public void invokeOnSuccess(JobEntity job) {
    invoke(
        job,
        job.getOnSuccessPayload(),
        "onSuccess",
        Collections.singletonList(JobContext.currentOrNull()));
  }

  public void invokeOnFailure(JobEntity job, Throwable failure) {
    invoke(
        job,
        job.getOnFailurePayload(),
        "onFailure",
        Arrays.asList(JobContext.currentOrNull(), failure));
  }

  private void invoke(
      JobEntity job, JobPayload callbackPayload, String callbackName, List<Object> runtimeArgs) {
    if (callbackPayload == null) {
      return;
    }
    try {
      validationFacade.validateSecurity(callbackPayload);
      JobPayload invocationPayload =
          payloadInvoker.materializeArguments(callbackPayload, payloadSerializer);
      if (invocationPayload.runtimeArgIndexes() != null) {
        List<Object> args = new ArrayList<>(invocationPayload.args());
        for (int i = 0; i < args.size(); i++) {
          Integer index = invocationPayload.runtimeArgIndexes().get(i);
          if (index == null) {
            continue;
          }
          if (index < 0 || index >= runtimeArgs.size()) {
            throw new IllegalStateException(
                callbackName
                    + " callback expects runtime parameter "
                    + index
                    + " but only "
                    + runtimeArgs.size()
                    + " are supplied");
          }
          args.set(i, runtimeArgs.get(index));
        }
        invocationPayload =
            new JobPayload(
                invocationPayload.target(),
                invocationPayload.method(),
                invocationPayload.methodDescriptor(),
                invocationPayload.isStatic(),
                args,
                invocationPayload.runtimeArgIndexes());
      }
      payloadInvoker.invoke(invocationPayload);
    } catch (Exception e) {
      // Log + metric + event; preserve the parent job outcome.
      log.errorf(
          e,
          "Job %s %s callback failed: %s: %s",
          job.getId(),
          callbackName,
          e.getClass().getName(),
          e.getMessage());
      try {
        observabilityFacade.recordCallbackFailure(job, e, 1);
      } catch (Exception metricEx) {
        log.warnf("Callback metric error for job %s: %s", job.getId(), metricEx.getMessage());
      }
      try {
        JobCallbackFailedEvent.CallbackType type =
            "onSuccess".equals(callbackName)
                ? JobCallbackFailedEvent.CallbackType.ON_SUCCESS
                : JobCallbackFailedEvent.CallbackType.ON_FAILURE;
        observabilityFacade.publishEvent(
            new JobCallbackFailedEvent(
                job.getId(),
                job.getBusinessKey(),
                job.getRecurringMasterId(),
                job.getPublicJobType(),
                job.getPriority(),
                job.getPickedBy(),
                clock.instant(),
                type,
                e.getMessage(),
                e.getClass().getName(),
                1));
      } catch (Exception eventEx) {
        log.warnf("Callback event publish error for job %s: %s", job.getId(), eventEx.getMessage());
      }
    }
  }

  /** Binds and clears job context for callers without a context on their current thread. */
  public void invokeOnFailureInJobContext(JobEntity job, Throwable failure) {
    if (job.getOnFailurePayload() == null) {
      return;
    }
    try {
      JobMdcContext.bindJobContext(
          job.getId(), job.getParams(), job.getPickedBy(), job.getCallerPrincipal());
      invokeOnFailure(job, failure);
    } finally {
      JobMdcContext.clear();
    }
  }
}
