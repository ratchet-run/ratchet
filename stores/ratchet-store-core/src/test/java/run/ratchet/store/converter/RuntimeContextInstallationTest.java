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
package run.ratchet.store.converter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PayloadMaskingPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.util.PayloadMaskingPolicyHolder;

class RuntimeContextInstallationTest {
  @Test
  void canceledRunnerRetainsOwnershipUntilItActuallyExits() {
    PayloadSerializer original = PayloadSerializerHolder.get();
    PayloadSerializer serializer = mock(PayloadSerializer.class);
    AtomicBoolean exited = new AtomicBoolean();
    RuntimeContextInstallation owner =
        new RuntimeContextInstallation(serializer, null, List.of(), null, null, false);
    owner.releaseWhen(exited::get);
    try {
      owner.close();
      assertSame(serializer, PayloadSerializerHolder.get());
      assertThrows(IllegalStateException.class, RuntimeContextInstallation::claim);
      exited.set(true);
      owner.releaseIfRequested();
      assertSame(original, PayloadSerializerHolder.get());
      try (RuntimeContextInstallation next = RuntimeContextInstallation.claim()) {
        next.setSerializer(mock(PayloadSerializer.class));
        owner.close();
        assertNotSame(serializer, PayloadSerializerHolder.get());
      }
    } finally {
      exited.set(true);
      owner.close();
    }
  }

  @Test
  void competingContextCannotReplaceOrReleaseOwnerAndSequentialContextCanStart() {
    PayloadSerializer original = PayloadSerializerHolder.get();
    PayloadMaskingPolicy originalMasking = PayloadMaskingPolicyHolder.get();
    PayloadSerializer first = mock(PayloadSerializer.class);
    PayloadSerializer second = mock(PayloadSerializer.class);
    RuntimeContextInstallation owner =
        new RuntimeContextInstallation(first, null, List.of(), null, null, false);
    try {
      assertSame(first, PayloadSerializerHolder.get());
      assertThrows(IllegalStateException.class, () -> PayloadSerializerHolder.set(second));
      assertThrows(IllegalStateException.class, () -> PayloadMaskingPolicyHolder.set(null));
      assertThrows(IllegalStateException.class, EncryptionHolder::disable);
      assertThrows(
          IllegalStateException.class,
          () -> new RuntimeContextInstallation(second, null, List.of(), null, null, false));
      assertSame(first, PayloadSerializerHolder.get());
    } finally {
      owner.close();
    }
    assertSame(original, PayloadSerializerHolder.get());
    assertSame(originalMasking, PayloadMaskingPolicyHolder.get());
    try (RuntimeContextInstallation next =
        new RuntimeContextInstallation(second, null, List.of(), null, null, false)) {
      owner.close();
      assertSame(second, PayloadSerializerHolder.get());
    }
    assertSame(original, PayloadSerializerHolder.get());
  }
}
