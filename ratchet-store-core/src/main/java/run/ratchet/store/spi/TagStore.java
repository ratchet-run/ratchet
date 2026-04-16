package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobStatus;
import java.util.List;
import java.util.Map;

/** Job tag management operations. */
@Incubating
public interface TagStore {

  void insertTags(long jobId, List<String> tags);

  int deleteTagsByJobId(long jobId);

  List<Long> findJobIdsByTag(String tag, int limit, int offset);

  Map<JobStatus, Long> countJobsByStatusForTag(String tag);

  Map<String, Long> countJobsByParamForTag(String tag, String paramKey);

  Map<String, Long> countJobsByExecutionNodeForTag(String tag);
}
