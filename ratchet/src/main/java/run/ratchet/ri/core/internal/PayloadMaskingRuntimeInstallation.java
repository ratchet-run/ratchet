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
package run.ratchet.ri.core.internal;

import java.util.List;
import java.util.Objects;
import run.ratchet.spi.PayloadMaskingPolicy;
import run.ratchet.store.util.PayloadMaskingPolicyHolder;

/** Installs an unambiguous payload masking policy, or preserves the built-in default. */
public final class PayloadMaskingRuntimeInstallation implements RuntimeInstallation {

  private final PayloadMaskingPolicy policy;

  public PayloadMaskingRuntimeInstallation(List<PayloadMaskingPolicy> policies) {
    List<PayloadMaskingPolicy> candidates =
        List.copyOf(Objects.requireNonNull(policies, "policies"));
    policy = candidates.size() == 1 ? candidates.get(0) : null;
  }

  @Override
  public void install(Object ownerToken) {
    PayloadMaskingPolicyHolder.install(ownerToken, policy);
  }

  @Override
  public void uninstall(Object ownerToken) {
    PayloadMaskingPolicyHolder.uninstall(ownerToken);
  }
}
