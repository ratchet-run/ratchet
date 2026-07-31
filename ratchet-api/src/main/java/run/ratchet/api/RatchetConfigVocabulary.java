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
package run.ratchet.api;

import java.util.List;
import run.ratchet.api.internal.RatchetConfigKeys;
import run.ratchet.spi.RatchetConfigKey;

/** Shared vocabulary for configuration-key families expanded at runtime. */
final class RatchetConfigVocabulary {

  static final List<ExecutionType> EXECUTION_TYPES =
      List.of(
          new ExecutionType("SINGLE", RatchetConfigKeys.THREAD_POOL_SIZE_SINGLE),
          new ExecutionType("RECURRING", RatchetConfigKeys.THREAD_POOL_SIZE_RECURRING),
          new ExecutionType("BATCH_CHILD", RatchetConfigKeys.THREAD_POOL_SIZE_BATCH_CHILD),
          new ExecutionType("BATCH_PARENT", RatchetConfigKeys.THREAD_POOL_SIZE_BATCH_PARENT),
          new ExecutionType("CHAIN_STEP", RatchetConfigKeys.THREAD_POOL_SIZE_CHAIN),
          new ExecutionType("WORKFLOW_BRANCH", RatchetConfigKeys.THREAD_POOL_SIZE_WORKFLOW_BRANCH),
          new ExecutionType("WORKFLOW_JOIN", RatchetConfigKeys.THREAD_POOL_SIZE_WORKFLOW_JOIN));

  static final List<CircuitBreakerProfile> CIRCUIT_BREAKER_PROFILES =
      List.of(CircuitBreakerProfile.values());

  private RatchetConfigVocabulary() {}

  record ExecutionType(String name, RatchetConfigKey<Integer> concurrencyKey) {}
}
