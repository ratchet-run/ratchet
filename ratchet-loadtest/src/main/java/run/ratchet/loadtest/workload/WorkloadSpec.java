package run.ratchet.loadtest.workload;

import java.io.Serializable;

public record WorkloadSpec(
    String runId,
    WorkloadType type,
    int sequence,
    long sleepMs,
    long sleepJitterMs,
    double sleepSpikeRate,
    long sleepSpikeMs,
    double failureRate,
    String payload)
    implements Serializable {}
