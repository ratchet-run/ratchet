package run.ratchet.loadtest.service;

import run.ratchet.loadtest.workload.WorkloadType;
import java.util.Locale;

public final class Tags {

  public static final String LOADTEST = "loadtest";
  public static final String PARAM_ENQUEUE_NODE = "loadtest.enqueue.node";
  private static final String RUN_PREFIX = "loadtest-run-";
  private static final String WORKLOAD_PREFIX = "workload-";
  private static final String ENQUEUE_NODE_PREFIX = "enqueue-node-";

  private Tags() {}

  public static String run(String runId) {
    return RUN_PREFIX + runId.toLowerCase(Locale.ROOT);
  }

  public static String workload(WorkloadType workload) {
    return WORKLOAD_PREFIX + workload.name().toLowerCase(Locale.ROOT).replace('_', '-');
  }

  public static String enqueueNode(String nodeId) {
    return ENQUEUE_NODE_PREFIX + safeTagValue(nodeId, 64 - ENQUEUE_NODE_PREFIX.length());
  }

  private static String safeTagValue(String raw, int maxLength) {
    String value = raw == null || raw.isBlank() ? "unknown" : raw.toLowerCase(Locale.ROOT);
    value = value.replaceAll("[^a-z0-9_-]", "-");
    if (value.length() <= maxLength) {
      return value;
    }
    String hash = Integer.toUnsignedString(value.hashCode(), 36);
    int prefixLength = Math.max(1, maxLength - hash.length() - 1);
    return value.substring(0, prefixLength) + "-" + hash;
  }
}
