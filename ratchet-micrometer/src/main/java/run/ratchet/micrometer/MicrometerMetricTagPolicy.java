package run.ratchet.micrometer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

  public static MicrometerMetricTagPolicy defaultPolicy() {
    return builder()
        .allowValues(
            "execution_type",
            "SINGLE",
            "RECURRING",
            "BATCH_PARENT",
            "BATCH_CHILD",
            "CHAIN_STEP",
            "DLQ_ALERT",
            "WORKFLOW_BRANCH",
            "WORKFLOW_JOIN")
        .allowValues("gate_status", "DRAINING", "RATE_LIMITED", "NO_PERMITS")
        .allowValues("source", "job_submit", "cluster_listener")
        .allowValues("transport", "jms", "none")
        .allowValues("outcome", "success", "failure", "skipped", "delivered", "ignored_self")
        .allowValues("outcome", "empty", "hit", "transient_failure")
        .allowValues("store", "mysql", "postgresql", "mongodb")
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
        .build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public MicrometerMetricTagPolicy and(MicrometerMetricTagPolicy additionalPolicy) {
    Objects.requireNonNull(additionalPolicy, "additionalPolicy must not be null");
    Map<String, Set<String>> merged = new HashMap<>();
    allowedValues.forEach((key, values) -> merged.put(key, new HashSet<>(values)));
    additionalPolicy.allowedValues.forEach(
        (key, values) -> merged.computeIfAbsent(key, ignored -> new HashSet<>()).addAll(values));
    return new MicrometerMetricTagPolicy(merged);
  }

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

  private static String normalizeTagName(String tagName) {
    return Objects.requireNonNull(tagName, "tagName must not be null")
        .trim()
        .toLowerCase(Locale.ROOT);
  }

  public static final class Builder {
    private final Map<String, Set<String>> allowedValues = new HashMap<>();

    private Builder() {}

    public Builder allowValue(String tagName, String value) {
      return allowValues(tagName, Set.of(value));
    }

    public Builder allowValues(String tagName, String... values) {
      return allowValues(tagName, Set.of(values));
    }

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

    public MicrometerMetricTagPolicy build() {
      return new MicrometerMetricTagPolicy(allowedValues);
    }
  }
}
