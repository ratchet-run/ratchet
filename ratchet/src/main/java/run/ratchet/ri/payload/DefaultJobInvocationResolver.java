package run.ratchet.ri.payload;

import jakarta.enterprise.context.ApplicationScoped;
import java.io.Serializable;
import java.util.List;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.JobInvocationResolver;

/** Default resolver using Ratchet's ASM lambda analysis. */
@ApplicationScoped
public class DefaultJobInvocationResolver implements JobInvocationResolver {

  @Override
  public JobInvocation resolve(Serializable callback) {
    return JobPayloadFactory.toInvocation(callback);
  }

  @Override
  public JobInvocation resolve(Serializable callback, List<Object> runtimeArguments) {
    return JobPayloadFactory.toInvocation(callback, runtimeArguments);
  }
}
