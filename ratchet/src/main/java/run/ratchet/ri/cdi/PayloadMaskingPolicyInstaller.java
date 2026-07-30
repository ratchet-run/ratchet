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
import run.ratchet.ri.core.internal.RuntimeInstallation;
import run.ratchet.spi.PayloadMaskingPolicy;
import run.ratchet.store.util.PayloadMaskingPolicyHolder;

/**
 * Installs the framework-resolved {@link PayloadMaskingPolicy} into {@link
 * PayloadMaskingPolicyHolder} at application startup, and clears it at shutdown so a redeploy does
 * not leak a stale policy across the static holder.
 *
 * <p>{@link PayloadMasker} lives in {@code store-core} and may run outside a CDI container, so it
 * resolves its policy through the static holder rather than {@code @Inject}. This installer is the
 * bridge: a deployer that produces its own {@link PayloadMaskingPolicy} bean overrides the built-in
 * default; when no bean is produced the holder keeps returning the built-in policy and behavior is
 * unchanged.
 *
 * <p>Resolution is best-effort and null-safe: if no unambiguous policy bean is available the holder
 * is left on its built-in default.
 */
@ApplicationScoped
public class PayloadMaskingPolicyInstaller {

  private final Instance<PayloadMaskingPolicy> policy;
  private volatile RuntimeInstallation runtimeInstallation;
  private volatile Object installedOwnerToken;

  /**
   * No-arg constructor so Weld can instantiate the client-proxy subclass (CDI 4.0 §3.15); never
   * used for a real instance, so the policy is left unset.
   */
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
        PayloadMaskingPolicy resolved =
            policy != null && policy.isResolvable() ? policy.get() : null;
        runtimeInstallation =
            new RuntimeInstallation() {
              @Override
              public void install(Object ownerToken) {
                PayloadMaskingPolicyHolder.install(ownerToken, resolved);
                installedOwnerToken = ownerToken;
              }

              @Override
              public void uninstall(Object ownerToken) {
                PayloadMaskingPolicyHolder.uninstall(ownerToken);
              }
            };
      }
      return runtimeInstallation;
    }
  }

  @PreDestroy
  void onShutdown() {
    Object ownerToken = installedOwnerToken;
    if (ownerToken != null) {
      PayloadMaskingPolicyHolder.uninstall(ownerToken);
    }
  }
}
