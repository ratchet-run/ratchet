package run.ratchet.ri.core;

import java.io.Serializable;
import run.ratchet.api.JobHandle;

interface BatchSubmitter {

  JobHandle submit(DefaultBatchBuilder builder);
}

interface StreamingBatchSubmitter {

  <T extends Serializable> JobHandle submit(DefaultStreamingBatchBuilder<T> builder);
}

interface RecurringJobSubmitter {

  JobHandle submit(DefaultRecurringJobBuilder builder);
}
