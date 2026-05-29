package run.ratchet.store.spi;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobStatus;

/** Job tag management operations. */
@Incubating
public interface TagStore {

  /**
   * Inserts tags for one job. Transaction attribute: {@code REQUIRED}.
   *
   * @param jobId job id receiving the tags; never {@code null}
   * @param tags tag names to attach; never {@code null}, may be empty (no-op when empty)
   */
  void insertTags(UUID jobId, List<String> tags);

  /**
   * Deletes tags for one job. Transaction attribute: {@code REQUIRED}.
   *
   * @param jobId job id whose tags should be removed; never {@code null}
   * @return number of tag rows deleted
   */
  int deleteTagsByJobId(UUID jobId);

  /**
   * Finds job ids for a tag. Transaction attribute: {@code SUPPORTS}.
   *
   * @param tag tag name to look up; never {@code null} or blank
   * @param limit maximum number of ids to return; must be positive
   * @param offset zero-based pagination offset; must be non-negative
   * @return ordered job ids carrying the tag, never {@code null}
   */
  List<UUID> findJobIdsByTag(String tag, int limit, int offset);

  /**
   * Counts jobs for one tag by status. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>This is a low-cardinality aggregation; implementations should group in the store.
   *
   * @param tag tag name to count against; never {@code null} or blank
   * @return per-status counts (omitted statuses have zero matches); never {@code null}
   */
  Map<JobStatus, Long> countJobsByStatusForTag(String tag);

  /**
   * Counts jobs for one tag by parameter value. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>Callers should use this for bounded diagnostic cardinalities, not arbitrary high-cardinality
   * payload fields.
   *
   * @param tag tag name to count against; never {@code null} or blank
   * @param paramKey job-parameter key whose distinct values become the result map keys; never
   *     {@code null} or blank
   * @return distinct-value counts keyed by parameter value (null parameter values map to {@code
   *     null}); never {@code null}
   */
  Map<String, Long> countJobsByParamForTag(String tag, String paramKey);

  /**
   * Counts jobs for one tag by execution node. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>This is bounded by scheduler-node cardinality.
   *
   * @param tag tag name to count against; never {@code null} or blank
   * @return per-node counts keyed by node id; never {@code null}
   */
  Map<String, Long> countJobsByExecutionNodeForTag(String tag);
}
