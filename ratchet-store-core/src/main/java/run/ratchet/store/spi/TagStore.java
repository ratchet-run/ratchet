package run.ratchet.store.spi;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobStatus;

/** Job tag management operations. */
@Incubating
public interface TagStore {

  /** Inserts tags for one job. Transaction attribute: {@code REQUIRED}. */
  void insertTags(UUID jobId, List<String> tags);

  /** Deletes tags for one job. Transaction attribute: {@code REQUIRED}. */
  int deleteTagsByJobId(UUID jobId);

  /** Finds job ids for a tag. Transaction attribute: {@code SUPPORTS}. */
  List<UUID> findJobIdsByTag(String tag, int limit, int offset);

  /**
   * Counts jobs for one tag by status. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>This is a low-cardinality aggregation; implementations should group in the store.
   */
  Map<JobStatus, Long> countJobsByStatusForTag(String tag);

  /**
   * Counts jobs for one tag by parameter value. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>Callers should use this for bounded diagnostic cardinalities, not arbitrary high-cardinality
   * payload fields.
   */
  Map<String, Long> countJobsByParamForTag(String tag, String paramKey);

  /**
   * Counts jobs for one tag by execution node. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>This is bounded by scheduler-node cardinality.
   */
  Map<String, Long> countJobsByExecutionNodeForTag(String tag);
}
