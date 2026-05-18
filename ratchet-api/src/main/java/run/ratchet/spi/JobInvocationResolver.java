package run.ratchet.spi;

import java.io.Serializable;
import java.util.List;
import run.ratchet.api.Incubating;

/** Converts user-submitted serializable callbacks into Ratchet job invocations. */
@Incubating
public interface JobInvocationResolver {

  /**
   * Resolves a serializable callback into a persisted invocation.
   *
   * <p>Implementations must reject invocations that target non-public methods and must not bypass
   * the scheduler's configured class policy. Returning an invocation is a claim that the target is
   * eligible for persistence and later execution.
   *
   * @param callback serializable user callback; never {@code null}
   * @return invocation descriptor suitable for persistence; never {@code null}
   * @throws IllegalArgumentException if the callback cannot be resolved into a valid invocation
   */
  JobInvocation resolve(Serializable callback);

  /**
   * Resolves a serializable callback with runtime-supplied arguments.
   *
   * @param callback serializable user callback; never {@code null}
   * @param runtimeArguments arguments appended or substituted by the caller; use an empty list when
   *     there are no runtime arguments. Implementations must treat this list as read-only and must
   *     not retain it unless they make their own copy.
   * @return invocation descriptor suitable for persistence; never {@code null}
   * @throws IllegalArgumentException if the callback cannot be resolved into a valid invocation
   */
  JobInvocation resolve(Serializable callback, List<Object> runtimeArguments);
}
