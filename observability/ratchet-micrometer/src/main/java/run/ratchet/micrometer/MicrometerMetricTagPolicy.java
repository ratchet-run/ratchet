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
package run.ratchet.micrometer;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.spi.ProtectedSurface;

/**
 * Allowlist for string-valued Micrometer tags emitted by {@link MicrometerMetricsCollector}.
 *
 * <p>Ratchet job tags can be arbitrary job metadata. Micrometer tags should stay bounded: each new
 * tag value can create a new meter/time series. Applications that intentionally want additional
 * metric tag values can provide a CDI bean of this type or pass one to the collector constructor.
 */
public final class MicrometerMetricTagPolicy {

  static final String UNKNOWN = "UNKNOWN";
  static final String OTHER = "OTHER";

  private final Map<String, Set<String>> allowedValues;

  private MicrometerMetricTagPolicy(Map<String, Set<String>> allowedValues) {
    Map<String, Set<String>> copy = new HashMap<>();
    allowedValues.forEach((key, values) -> copy.put(normalizeTagName(key), Set.copyOf(values)));
    this.allowedValues = Map.copyOf(copy);
  }

  /**
   * Returns the framework-shipped baseline allowlist. Covers every tag value the bundled {@link
   * MicrometerMetricsCollector} emits internally; deployments that need additional values compose
   * against this via {@link #and(MicrometerMetricTagPolicy)}.
   *
   * @return a freshly built immutable policy containing the framework defaults; never {@code null}
   */
  public static MicrometerMetricTagPolicy defaultPolicy() {
    return builder()
        .allowValues(
            "execution_type",
            "SINGLE",
            "RECURRING",
            "BATCH_PARENT",
            "BATCH_CHILD",
            "CHAIN_STEP",
            "WORKFLOW_BRANCH",
            "WORKFLOW_JOIN")
        .allowValues("gate_status", "DRAINING", "RATE_LIMITED", "NO_PERMITS")
        .allowValues("source", "job_submit", "cluster_listener")
        .allowValues("transport", "jms", "none")
        .allowValues("outcome", "success", "failure", "skipped", "delivered", "ignored_self")
        .allowValues("outcome", "empty", "hit", "transient_failure")
        .allowValues("store", "mysql", "postgresql", "oracle", "sqlserver", "mongodb")
        .allowValues(
            "operation",
            "claim_lookup",
            "claim_mark_running_batch",
            "compare_and_swap_status",
            "increment_retry_attempt",
            "mark_succeeded",
            "mark_succeeded_minimal",
            "pickup_job",
            "reset_running_job",
            "reset_running_jobs",
            "schedule_retry",
            "update_status")
        .allowValue("breaker", "store.claim")
        .allowValue("service", "store.claim")
        .allowValues(
            "profile",
            EnumSet.allOf(CircuitBreakerProfile.class).stream()
                .map(CircuitBreakerProfile::name)
                .toList())
        .allowValues("requested_target", "platform", "virtual")
        .allowValues("effective_target", "platform")
        .allowValues("version_gap", "next", "multiple_versions_ahead", "not_newer")
        .allowValues(
            "surface",
            Arrays.stream(ProtectedSurface.values()).map(ProtectedSurface::name).toList())
        .build();
  }

  /**
   * Starts a fresh {@link Builder} with no pre-allowed values. Use {@link #defaultPolicy()} when
   * the framework defaults should be retained.
   *
   * @return a new empty builder; never {@code null}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a new policy whose allowlist is the union of this policy's values and {@code
   * additionalPolicy}'s values, tag-by-tag.
   *
   * @param additionalPolicy policy whose entries are merged into this one; must not be {@code null}
   * @return a freshly built immutable composite policy; never {@code null}
   * @throws NullPointerException if {@code additionalPolicy} is {@code null}
   */
  public MicrometerMetricTagPolicy and(MicrometerMetricTagPolicy additionalPolicy) {
    Objects.requireNonNull(additionalPolicy, "additionalPolicy must not be null");
    Map<String, Set<String>> merged = new HashMap<>();
    allowedValues.forEach((key, values) -> merged.put(key, new HashSet<>(values)));
    additionalPolicy.allowedValues.forEach(
        (key, values) -> merged.computeIfAbsent(key, ignored -> new HashSet<>()).addAll(values));
    return new MicrometerMetricTagPolicy(merged);
  }

  /**
   * Resolves a raw tag value into the value that should actually be emitted on the meter.
   *
   * @param tagName Micrometer tag name (case-insensitive); must not be {@code null}
   * @param rawValue raw tag value supplied by the caller; {@code null} or blank values resolve to
   *     {@link #UNKNOWN}
   * @return {@code rawValue} when the tag's allowlist contains it; {@link #UNKNOWN} when {@code
   *     rawValue} is {@code null} or blank; {@link #OTHER} for any other unrecognised value
   * @throws NullPointerException if {@code tagName} is {@code null}
   */
  public String metricTagValue(String tagName, String rawValue) {
    if (rawValue == null || rawValue.isBlank()) {
      return UNKNOWN;
    }
    Set<String> allowed = allowedValues.get(normalizeTagName(tagName));
    if (allowed != null && allowed.contains(rawValue)) {
      return rawValue;
    }
    return OTHER;
  }

  boolean explicitlyAllows(String tagName, String rawValue) {
    if (rawValue == null || rawValue.isBlank()) {
      return false;
    }
    Set<String> allowed = allowedValues.get(normalizeTagName(tagName));
    return allowed != null && allowed.contains(rawValue);
  }

  private static String normalizeTagName(String tagName) {
    return Objects.requireNonNull(tagName, "tagName must not be null")
        .trim()
        .toLowerCase(Locale.ROOT);
  }

  public static final class Builder {
    private final Map<String, Set<String>> allowedValues = new HashMap<>();

    private Builder() {}

    /**
     * Allows a single value for the given tag.
     *
     * @param tagName tag name (case-insensitive); must not be {@code null}
     * @param value allowed value; must not be {@code null} or blank
     * @return this builder
     * @throws IllegalArgumentException if {@code value} is {@code null} or blank
     */
    public Builder allowValue(String tagName, String value) {
      return allowValues(tagName, Set.of(value));
    }

    /**
     * Allows the supplied values for the given tag.
     *
     * @param tagName tag name (case-insensitive); must not be {@code null}
     * @param values allowed values; none may be {@code null} or blank
     * @return this builder
     * @throws IllegalArgumentException if any value is {@code null} or blank
     */
    public Builder allowValues(String tagName, String... values) {
      return allowValues(tagName, Set.of(values));
    }

    /**
     * Allows the supplied values for the given tag.
     *
     * @param tagName tag name (case-insensitive); must not be {@code null}
     * @param values allowed values; none may be {@code null} or blank
     * @return this builder
     * @throws IllegalArgumentException if any value is {@code null} or blank
     */
    public Builder allowValues(String tagName, Collection<String> values) {
      String normalizedTag = normalizeTagName(tagName);
      Set<String> tagValues =
          allowedValues.computeIfAbsent(normalizedTag, ignored -> new HashSet<>());
      for (String value : values) {
        if (value == null || value.isBlank()) {
          throw new IllegalArgumentException("Micrometer tag allowlist values must not be blank");
        }
        tagValues.add(value);
      }
      return this;
    }

    /**
     * Materialises the configured allowlist into a freshly built immutable policy.
     *
     * @return a new immutable policy reflecting the builder's current state; never {@code null}
     */
    public MicrometerMetricTagPolicy build() {
      return new MicrometerMetricTagPolicy(allowedValues);
    }
  }
}
