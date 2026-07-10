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
package run.ratchet.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;

/**
 * Worker-side hook that can patch a job's invocation arguments at the last moment before reflective
 * dispatch — after security validation, before argument coercion. Extensions use it for late
 * binding: resolving placeholder arguments against state that did not exist at submission time (for
 * example, upstream step results in a workflow).
 *
 * <p><b>Arguments only.</b> The dispatched target class, method, and descriptor are pinned by the
 * payload that security validation cleared; only {@link JobInvocation#arguments()} of the returned
 * invocation are honored. Returning {@code null} or the input instance leaves the arguments
 * untouched.
 *
 * <p><b>Scope.</b> The hook runs for the job's main invocation. Success/failure callback payloads
 * and workflow-condition predicates follow their own dispatch paths and are not resolved.
 *
 * <p>When no bean of this type is registered, the worker dispatches the persisted arguments
 * unchanged. A thrown exception fails the executing job through the normal failure path (retry
 * policy applies).
 */
@Incubating
public interface PreExecutionArgResolver {

  /**
   * Resolves the arguments to dispatch for one job execution.
   *
   * @param jobId the executing job's id; never {@code null}
   * @param invocation the persisted invocation about to be dispatched; never {@code null}
   * @return an invocation whose arguments replace the persisted ones, or {@code null} / the input
   *     instance to dispatch unchanged
   */
  JobInvocation resolveArguments(UUID jobId, JobInvocation invocation);
}
