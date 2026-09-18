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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import jakarta.enterprise.inject.Instance;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PayloadMaskingPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.RuntimeContextInstallation;
import run.ratchet.store.util.PayloadMaskingPolicyHolder;

class CdiRuntimeContextInstallationTest {
  @Test
  @SuppressWarnings("unchecked")
  void cdiInstallerClaimsSharedOwnerBeforeSpringCanInstall() {
    PayloadMaskingPolicy policy = mock(PayloadMaskingPolicy.class);
    Instance<PayloadMaskingPolicy> policies = mock(Instance.class);
    when(policies.isResolvable()).thenReturn(true);
    when(policies.get()).thenReturn(policy);
    CdiRuntimeContextInstallation owner = new CdiRuntimeContextInstallation();
    PayloadMaskingPolicyInstaller installer = new PayloadMaskingPolicyInstaller(policies);
    installer.runtimeInstallation = owner;
    try {
      installer.onStartup(new Object());
      assertSame(policy, PayloadMaskingPolicyHolder.get());
      assertThrows(
          IllegalStateException.class,
          () ->
              new RuntimeContextInstallation(
                  mock(PayloadSerializer.class), null, List.of(), null, null, false));
      assertSame(policy, PayloadMaskingPolicyHolder.get());
    } finally {
      owner.close();
      installer.onShutdown();
    }
    try (RuntimeContextInstallation spring =
        new RuntimeContextInstallation(
            mock(PayloadSerializer.class), policy, List.of(), null, null, false)) {
      installer.onShutdown();
      assertSame(policy, PayloadMaskingPolicyHolder.get());
    }
  }
}
