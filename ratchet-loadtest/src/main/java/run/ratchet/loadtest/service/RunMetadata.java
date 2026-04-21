package run.ratchet.loadtest.service;

import java.time.Instant;

public record RunMetadata(String runId, String workload, int expectedJobs, Instant startedAt) {}
