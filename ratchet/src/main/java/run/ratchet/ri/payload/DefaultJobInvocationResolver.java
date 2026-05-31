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
