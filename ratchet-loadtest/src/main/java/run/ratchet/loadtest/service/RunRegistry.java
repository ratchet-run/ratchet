package run.ratchet.loadtest.service;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class RunRegistry {

  private final ConcurrentMap<String, RunMetadata> runs = new ConcurrentHashMap<>();

  public void put(RunMetadata metadata) {
    runs.put(metadata.runId(), metadata);
  }

  public Optional<RunMetadata> get(String runId) {
    return Optional.ofNullable(runs.get(runId));
  }

  public List<RunMetadata> all() {
    return new ArrayList<>(runs.values());
  }

  public void remove(String runId) {
    runs.remove(runId);
  }

  public void clear() {
    runs.clear();
  }
}
