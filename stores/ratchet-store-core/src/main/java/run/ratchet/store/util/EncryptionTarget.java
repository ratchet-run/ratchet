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
package run.ratchet.store.util;

import java.util.UUID;
import run.ratchet.spi.ProtectedSurface;

/**
 * Bundles the surface and binding identity a call site supplies to {@link PayloadEncryptor}, so the
 * per-surface AAD policy lives in one place rather than being re-derived at every call site.
 *
 * <p>The factories encode the policy:
 *
 * <ul>
 *   <li>{@link #rowBound} binds the owning job id — for a job's own payload args, parameter values,
 *       result, and callback payloads;
 *   <li>{@link #signal} binds the signal key — a broadcast writes one ciphertext to every waiting
 *       row matching the key, so the key (not a single job id) is the identity every targeted row
 *       shares;
 *   <li>{@link #predicate} binds the parent job id — in scope at both the write site and the
 *       evaluation site.
 * </ul>
 *
 * @param surface the protected surface
 * @param jobId the job id passed to the engine on the {@code EncryptionContext}; {@code null} for
 *     the signal surface (whose binding is the key, carried in {@code binding})
 * @param binding the AAD binding bytes for this surface (see {@link EncryptionAad})
 */
public record EncryptionTarget(ProtectedSurface surface, UUID jobId, byte[] binding) {

  /** A job-bound surface (payload args, parameter values, result, callback payloads). */
  public static EncryptionTarget rowBound(ProtectedSurface surface, UUID jobId) {
    return new EncryptionTarget(surface, jobId, EncryptionAad.binding(jobId));
  }

  /** The signal-payload surface, bound to the signal key. */
  public static EncryptionTarget signal(String signalKey) {
    return new EncryptionTarget(
        ProtectedSurface.SIGNAL_PAYLOAD, null, EncryptionAad.binding(signalKey));
  }

  /** The workflow-condition-predicate surface, bound to the parent job id. */
  public static EncryptionTarget predicate(UUID parentJobId) {
    return new EncryptionTarget(
        ProtectedSurface.WORKFLOW_CONDITION_PREDICATE,
        parentJobId,
        EncryptionAad.binding(parentJobId));
  }
}
