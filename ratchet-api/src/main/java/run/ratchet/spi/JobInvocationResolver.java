/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.spi;

import java.io.Serializable;
import java.util.List;
import run.ratchet.api.Incubating;

/**
 * Converts user-submitted serializable callbacks into Ratchet job invocations.
 *
 * <p><b>Class-policy enforcement is not the resolver's responsibility.</b> Implementations are
 * expected to translate a serializable callback into a structural {@link JobInvocation} descriptor;
 * the framework enforces the configured {@code ClassPolicy} elsewhere in the execution pipeline
 * (notably in {@code JobTask} and {@code JobSecurityValidator}, which gate dispatch and
 * deserialization). Implementations MAY consult policy if they choose, but resolvers that omit a
 * policy check still produce contract-compliant invocations.
 *
 * <p>Implementations MUST still reject invocations whose structural shape is invalid (non-public
 * methods, missing target class, malformed argument lists). Returning an invocation is a claim that
 * the target is structurally eligible for persistence and later execution, not that it has been
 * cleared by the deployment's class policy.
 */
@Incubating
public interface JobInvocationResolver {

  /**
   * Resolves a serializable callback into a persisted invocation.
   *
   * <p>Implementations MUST reject invocations that target non-public methods or otherwise cannot
   * be structurally translated into a {@link JobInvocation}. See the interface-level Javadoc for
   * the division of responsibility between this SPI and the framework's class-policy enforcement.
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
