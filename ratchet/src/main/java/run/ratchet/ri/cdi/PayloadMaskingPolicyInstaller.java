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
package run.ratchet.ri.cdi;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import run.ratchet.ri.core.internal.PayloadMaskingRuntimeInstallation;
import run.ratchet.ri.core.internal.RuntimeInstallation;
import run.ratchet.spi.PayloadMaskingPolicy;
import run.ratchet.store.util.PayloadMaskingPolicyHolder;

/**
 * Installs the framework-resolved {@link PayloadMaskingPolicy} into {@link
 * PayloadMaskingPolicyHolder} while delegating holder behavior to the neutral runtime installation.
 */
@ApplicationScoped
public class PayloadMaskingPolicyInstaller {

  private final Instance<PayloadMaskingPolicy> policy;
  private volatile RuntimeInstallation runtimeInstallation;
  private volatile Object installedOwnerToken;

  /** Weld client-proxy constructor; never used for a real contextual instance. */
  protected PayloadMaskingPolicyInstaller() {
    this.policy = null;
  }

  @Inject
  public PayloadMaskingPolicyInstaller(Instance<PayloadMaskingPolicy> policy) {
    this.policy = policy;
  }

  public RuntimeInstallation runtimeInstallation() {
    RuntimeInstallation current = runtimeInstallation;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      if (runtimeInstallation == null) {
        RuntimeInstallation delegate =
            new PayloadMaskingRuntimeInstallation(
                policy == null ? java.util.List.of() : policy.stream().toList());
        runtimeInstallation =
            new RuntimeInstallation() {
              @Override
              public void install(Object ownerToken) {
                delegate.install(ownerToken);
                installedOwnerToken = ownerToken;
              }

              @Override
              public void uninstall(Object ownerToken) {
                delegate.uninstall(ownerToken);
              }
            };
      }
      return runtimeInstallation;
    }
  }

  @PreDestroy
  void onShutdown() {
    RuntimeInstallation current = runtimeInstallation;
    Object ownerToken = installedOwnerToken;
    if (current != null && ownerToken != null) {
      current.uninstall(ownerToken);
    }
  }
}
