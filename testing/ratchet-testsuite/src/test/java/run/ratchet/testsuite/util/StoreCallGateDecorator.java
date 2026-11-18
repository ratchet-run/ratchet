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
package run.ratchet.testsuite.util;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.api.Nullable;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;

/**
 * Passes covered store calls through {@link StoreCallGate}. It is a decorator because the engine
 * creates the store bean during deployment, before a component invocation context exists, and
 * GlassFish 8 then fails the deployment while creating an application interceptor for it. Payara is
 * built on the same GlassFish code.
 *
 * <p>It runs inside the store's own interceptors. A paused call is already inside the store's
 * {@code @Transactional} REQUIRED boundary, which joins the caller's transaction when there is one.
 */
@Decorator
@Priority(Interceptor.Priority.APPLICATION)
public abstract class StoreCallGateDecorator implements JobStore {
  @Inject @Delegate JobStore delegate;

  @Override
  public <T> Optional<T> capability(Class<T> type) {
    // Let the store answer regardless of how the container handles decorator default methods.
    return delegate.capability(type);
  }

  @Override
  public Optional<JobEntity> findById(UUID id) {
    StoreCallGate.Pause fired = StoreCallGate.enter("findById", id);
    boolean completed = false;
    Object returned = null;
    try {
      Optional<JobEntity> result = delegate.findById(id);
      returned = result;
      completed = true;
      return result;
    } finally {
      StoreCallGate.exit("findById", id, fired, completed, returned);
    }
  }

  @Override
  public @Nullable JobStatus getJobStatus(UUID id) {
    StoreCallGate.Pause fired = StoreCallGate.enter("getJobStatus", id);
    boolean completed = false;
    Object returned = null;
    try {
      JobStatus result = delegate.getJobStatus(id);
      returned = result;
      completed = true;
      return result;
    } finally {
      StoreCallGate.exit("getJobStatus", id, fired, completed, returned);
    }
  }

  @Override
  public int incrementRetryAttempt(UUID id, @Nullable Long expectedClaimSeq) {
    StoreCallGate.Pause fired = StoreCallGate.enter("incrementRetryAttempt", id);
    boolean completed = false;
    Object returned = null;
    try {
      int result = delegate.incrementRetryAttempt(id, expectedClaimSeq);
      returned = result;
      completed = true;
      return result;
    } finally {
      StoreCallGate.exit("incrementRetryAttempt", id, fired, completed, returned);
    }
  }

  @Override
  public boolean scheduleJobRetry(
      UUID id,
      String error,
      Instant newScheduledTime,
      int attempts,
      @Nullable Long expectedClaimSeq) {
    StoreCallGate.Pause fired = StoreCallGate.enter("scheduleJobRetry", id);
    boolean completed = false;
    Object returned = null;
    try {
      boolean result =
          delegate.scheduleJobRetry(id, error, newScheduledTime, attempts, expectedClaimSeq);
      returned = result;
      completed = true;
      return result;
    } finally {
      StoreCallGate.exit("scheduleJobRetry", id, fired, completed, returned);
    }
  }

  @Override
  public boolean releaseBusinessKeyHold(UUID jobId, long claimSeq) {
    StoreCallGate.Pause fired = StoreCallGate.enter("releaseBusinessKeyHold", jobId);
    boolean completed = false;
    Object returned = null;
    try {
      boolean result = delegate.releaseBusinessKeyHold(jobId, claimSeq);
      returned = result;
      completed = true;
      return result;
    } finally {
      StoreCallGate.exit("releaseBusinessKeyHold", jobId, fired, completed, returned);
    }
  }
}
