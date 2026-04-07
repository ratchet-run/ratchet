package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import java.util.List;

/** Job tag management operations. */
@Incubating
public interface TagStore {

  /** Inserts all supplied tags for a job. */
  void insertTags(long jobId, List<String> tags);

  /** Deletes all tags associated with a job and returns the number removed. */
  int deleteTagsByJobId(long jobId);

  /** Returns job IDs matching a tag using store-defined pagination semantics. */
  List<Long> findJobIdsByTag(String tag, int limit, int offset);
}
