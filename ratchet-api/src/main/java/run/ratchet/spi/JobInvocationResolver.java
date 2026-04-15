package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.io.Serializable;
import java.util.List;

/** Converts user-submitted serializable callbacks into Ratchet job invocations. */
@Incubating
public interface JobInvocationResolver {

  JobInvocation resolve(Serializable callback);

  JobInvocation resolve(Serializable callback, List<Object> runtimeArguments);
}
