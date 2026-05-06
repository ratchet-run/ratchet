package run.ratchet.store.spi;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobStatus;

/** Job tag management operations. */
@Incubating
public interface TagStore {

  void insertTags(UUID jobId, List<String> tags);

  int deleteTagsByJobId(UUID jobId);

  List<UUID> findJobIdsByTag(String tag, int limit, int offset);

  Map<JobStatus, Long> countJobsByStatusForTag(String tag);

  Map<String, Long> countJobsByParamForTag(String tag, String paramKey);

  Map<String, Long> countJobsByExecutionNodeForTag(String tag);
}
