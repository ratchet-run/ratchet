package run.ratchet.spi;

import java.io.Serializable;
import java.util.List;
import run.ratchet.api.Incubating;

/** Converts user-submitted serializable callbacks into Ratchet job invocations. */
@Incubating
public interface JobInvocationResolver {

  JobInvocation resolve(Serializable callback);

  JobInvocation resolve(Serializable callback, List<Object> runtimeArguments);
}
