package run.ratchet.store.spi;

import java.util.List;

/** Job tag management operations. */
public interface TagStore {

  void insertTags(long jobId, List<String> tags);

  int deleteTagsByJobId(long jobId);

  List<Long> findJobIdsByTag(String tag, int limit, int offset);
}
