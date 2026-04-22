package run.ratchet.ri.core;

import run.ratchet.api.JobHandle;
import java.io.Serializable;

interface BatchSubmitter {

  JobHandle submit(DefaultBatchBuilder builder);
}

interface StreamingBatchSubmitter {

  <T extends Serializable> JobHandle submit(DefaultStreamingBatchBuilder<T> builder);
}

interface RecurringJobSubmitter {

  JobHandle submit(DefaultRecurringJobBuilder builder);
}
