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

import run.ratchet.api.Incubating;

/**
 * The closed set of job-data surfaces that payload encryption protects.
 *
 * <p>This enum <em>is</em> the protected boundary, expressed in code rather than prose: a value
 * stored in a surface named here is encrypted when the owning job opts in; every other column —
 * routing keys, correlation metadata, timestamps, parameter <em>keys</em> — is cleartext by design
 * so that claim, query, dedup, and tracing keep working. Adding a member is the additive way to
 * widen the boundary; nothing outside this set is ever handed to a {@link PayloadEncryption} engine.
 *
 * <p>The surface also drives the additional-authenticated-data (AAD) policy. Most surfaces bind
 * their ciphertext to the job id so a value lifted from one row fails to decrypt in another, but two
 * surfaces cannot and bind to the surface alone — see {@link #SIGNAL_PAYLOAD} and {@link
 * #WORKFLOW_CONDITION_PREDICATE}. The framework computes the final AAD bytes from this surface and
 * hands them to the engine; the engine never recomputes them.
 */
@Incubating
public enum ProtectedSurface {

  /** The {@code args} sub-tree of a job's {@code payload}. AAD binds surface and job id. */
  PAYLOAD_ARGS,

  /** A value in a job's {@code params} map (the keys stay cleartext). AAD binds surface and job id. */
  PARAM_VALUE,

  /** A persisted job result ({@code job_result}). AAD binds surface and job id. */
  RESULT,

  /** The success-callback payload ({@code on_success_payload}). AAD binds surface and job id. */
  ON_SUCCESS_PAYLOAD,

  /** The failure-callback payload ({@code on_failure_payload}). AAD binds surface and job id. */
  ON_FAILURE_PAYLOAD,

  /**
   * A delivered signal payload ({@code signal_payload}). AAD binds the surface only: broadcast
   * delivery writes one ciphertext to every waiting row matching the signal key, so binding to a
   * single job id would fail the authentication tag on all rows but one.
   */
  SIGNAL_PAYLOAD,

  /**
   * A serialized workflow-condition predicate ({@code condition_expression}). AAD binds the surface
   * only: the predicate is decrypted at a call site that has no job id in scope, and a predicate
   * belongs to exactly one parent job and is never shared, so surface-only binding is acceptable.
   */
  WORKFLOW_CONDITION_PREDICATE
}
