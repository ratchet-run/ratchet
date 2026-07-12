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
package run.ratchet.api;

import java.util.List;
import java.util.Objects;

/**
 * Fluent builder for configuring and submitting recurring jobs.
 *
 * @apiNote Unlike {@link JobBuilder}, this builder is intentionally submit-only and does not expose
 *     read-back accessors for the configured options, tags, or business key. Tooling that needs to
 *     inspect a recurring schedule after submission should query {@link JobQueryService} using the
 *     returned {@link JobHandle}.
 * @see JobOptions
 * @see JobHandle
 * @since 0.1
 */
@Incubating
public interface RecurringJobBuilder {

  /**
   * Replaces the recurring job options used for children created from this schedule.
   *
   * <p>The default is {@link JobOptions#defaults()}. This is an in-memory builder operation; it
   * does not open a transaction.
   *
   * @param options non-null options to apply
   * @throws NullPointerException if {@code options} is null
   */
  RecurringJobBuilder withOptions(JobOptions options);

  /**
   * Replaces the recurring job tags.
   *
   * <p>The builder defensively copies the supplied list. Passing null clears the tags. Duplicate
   * tags are not significant; backing stores may collapse duplicates when persisting tags.
   *
   * @param tags replacement tags, or null for no tags
   * @throws NullPointerException if {@code tags} contains null elements
   */
  RecurringJobBuilder withTags(List<String> tags);

  /**
   * Sets the business key for active-unique identity. While the job is active (PENDING, RUNNING,
   * PAUSED), no other job may share the same key. For {@link Recurring @Recurring} methods, the
   * annotation's {@link Recurring#id() id} is used automatically.
   *
   * <p>After trimming, the key must contain at most 255 printable ASCII characters ({@code U+0020}
   * through {@code U+007E}). Invalid keys are rejected rather than truncated or converted by a
   * store.
   *
   * @param key the business key, or null/blank for none
   * @throws IllegalArgumentException if the normalized key is too long or contains a character
   *     outside the portable subset
   */
  RecurringJobBuilder withBusinessKey(String key);

  /**
   * Sets the policy used when more than one cron occurrence is overdue after downtime.
   *
   * <p>The default is {@link RecurringMisfirePolicy#defaults()}, which preserves Ratchet's existing
   * bounded catch-up behavior. The policy is stored with the recurring master so every node applies
   * the same decision after restart.
   *
   * @param policy non-null misfire policy
   * @throws NullPointerException if {@code policy} is null
   * @throws UnsupportedOperationException if the builder does not support persisted misfire policy
   */
  default RecurringJobBuilder withMisfirePolicy(RecurringMisfirePolicy policy) {
    Objects.requireNonNull(policy, "policy");
    throw new UnsupportedOperationException("Recurring misfire policies are not supported");
  }

  /**
   * Routes occurrences created from this recurring job to the virtual executor pool ({@link
   * ExecutorTargets#VIRTUAL}).
   *
   * <p>Mutually exclusive with {@link #platform()}; last call wins. Calling neither leaves
   * occurrences on the deployment's default threading mode. If no virtual executor is configured,
   * each occurrence falls back to the platform pool (observed via a metric and a one-time warning)
   * - the target selects a configured pool, not a guaranteed thread type.
   */
  RecurringJobBuilder virtual();

  /**
   * Routes occurrences created from this recurring job to the platform executor pool ({@link
   * ExecutorTargets#PLATFORM}).
   *
   * <p>Mutually exclusive with {@link #virtual()}; last call wins. Calling neither leaves
   * occurrences on the deployment's default threading mode. The platform pool is always present.
   */
  RecurringJobBuilder platform();

  /**
   * Encrypts the payload arguments and parameter values of children created from this schedule, and
   * the master's stored payload templates, at rest — the recurring-job equivalent of {@link
   * JobBuilder#withEncryptedPayload()}. Requires a {@code PayloadEncryption} engine and {@code
   * KeyProvider} to be installed; with none, submission fails fast rather than storing plaintext.
   * The deployment-wide encryption switch, when on, encrypts every job regardless of this call.
   *
   * @return this builder
   */
  RecurringJobBuilder withEncryptedPayload();

  /**
   * Persists the recurring job and returns a handle to it.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. Non-terminal builder methods are in-memory
   * only and do not participate in a transaction.
   */
  JobHandle submit();
}
