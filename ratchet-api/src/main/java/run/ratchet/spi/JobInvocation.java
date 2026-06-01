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

import java.util.List;
import run.ratchet.api.Incubating;

/**
 * Serializable description of the target method Ratchet should invoke for a job.
 *
 * @param targetClass fully qualified target class name
 * @param methodName target method name
 * @param methodDescriptor JVM method descriptor
 * @param staticMethod whether the invocation targets a static method
 * @param arguments persisted invocation arguments; {@code null} is normalized to an empty list
 */
@Incubating
public record JobInvocation(
    String targetClass,
    String methodName,
    String methodDescriptor,
    boolean staticMethod,
    List<Object> arguments) {

  public JobInvocation {
    arguments = arguments == null ? List.of() : List.copyOf(arguments);
  }
}
