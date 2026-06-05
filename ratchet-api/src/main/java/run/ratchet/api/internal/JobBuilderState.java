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
package run.ratchet.api.internal;

import java.time.Duration;
import java.util.List;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobContext;
import run.ratchet.api.SerializableBiConsumer;
import run.ratchet.api.SerializableCheckedRunnable;

/**
 * Framework-internal accessor view of a {@link JobBuilder} used by the Ratchet runtime to read
 * builder state during job creation.
 *
 * <p><b>Framework-internal:</b> applications must not depend on this interface and must not cast
 * {@link JobBuilder} to it. The accessors here expose builder-side mutable state for the reference
 * implementation and store layers; the public configuration surface is {@link JobBuilder}'s fluent
 * {@code with*} / {@code on*} methods.
 *
 * <p>Every {@link JobBuilder} implementation produced by the runtime also implements this
 * interface, so consumer code inside the framework may cast to it after obtaining a builder from
 * {@link run.ratchet.api.JobSubmitter}.
 *
 * @since 0.1.0
 */
public interface JobBuilderState {

  /**
   * Returns the signal key set via {@link JobBuilder#awaitSignal}, or {@code null} if the builder
   * is not configured to wait for a signal.
   *
   * @return the signal key, or {@code null} if absent
   */
  String awaitSignalKey();

  /**
   * Returns the signal timeout duration set via {@link JobBuilder#awaitSignal}, or {@code null} if
   * the builder is not configured to wait for a signal.
   *
   * @return the signal timeout, or {@code null} if absent
   */
  Duration awaitSignalTimeout();

  /**
   * Returns the chain tasks in addition order.
   *
   * @return an unmodifiable list of chain tasks (never {@code null}; may be empty)
   */
  List<SerializableCheckedRunnable> chainTasks();

  /**
   * Returns the delay duration; never {@code null} (may be {@link Duration#ZERO}).
   *
   * <p>The delay is fixed when the builder is created by a {@link run.ratchet.api.JobSubmitter}
   * enqueue overload; the fluent builder does not expose a delay mutator.
   *
   * @return the configured delay
   */
  Duration delay();

  /**
   * Returns the idempotency key; never {@code null} (auto-generated UUID if not overridden via
   * {@link JobBuilder#withIdempotencyKey(String)}).
   *
   * @return the effective idempotency key
   */
  String idempotencyKey();

  /**
   * Returns the business key, or {@code null} if not configured.
   *
   * @return the business key, or {@code null}
   */
  String businessKey();

  /**
   * Returns the failure callback, or {@code null} if not configured.
   *
   * @return the failure callback, or {@code null}
   */
  SerializableBiConsumer<JobContext, Throwable> onFailure();

  /**
   * Returns the execution-target label set via {@link JobBuilder#virtual()}, {@link
   * JobBuilder#platform()}, or {@code null} when the job inherits the deployment's default
   * threading mode.
   *
   * @return the execution target, or {@code null} when no explicit target was set
   */
  String executionTarget();

  /**
   * Returns whether this job opted in to payload encryption via {@link
   * JobBuilder#withEncryptedPayload()}.
   *
   * <p>The creation service later carries this onto the entity's {@code encrypted_payload} flag so
   * the write path knows to encrypt the job's protected surfaces.
   *
   * @return {@code true} if the job opted in to encryption
   */
  boolean encryptedPayload();
}
