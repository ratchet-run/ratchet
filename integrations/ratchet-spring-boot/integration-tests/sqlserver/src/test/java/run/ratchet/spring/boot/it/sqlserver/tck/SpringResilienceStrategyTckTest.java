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
package run.ratchet.spring.boot.it.sqlserver.tck;

import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.DefaultCircuitBreakerConfigProvider;
import run.ratchet.ri.resilience.DefaultResilienceStrategy;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.tck.api.AbstractResilienceStrategyContract;

/** Spring binding for {@link AbstractResilienceStrategyContract}. */
@SpringSqlserverTck
class SpringResilienceStrategyTckTest extends AbstractResilienceStrategyContract {

  private final CircuitBreakerConfigProvider configProvider =
      new DefaultCircuitBreakerConfigProvider(RatchetOptions.defaults());
  private final CircuitBreakerRegistry circuitBreakerRegistry =
      new CircuitBreakerRegistry(configProvider);
  private final ResilienceStrategy resilienceStrategy =
      new DefaultResilienceStrategy(circuitBreakerRegistry, configProvider);

  @Override
  protected ResilienceStrategy resilienceStrategy() {
    return resilienceStrategy;
  }

  @Override
  protected void forceOpenCircuit(String serviceName) {
    circuitBreakerRegistry.openBreaker(serviceName);
  }
}
