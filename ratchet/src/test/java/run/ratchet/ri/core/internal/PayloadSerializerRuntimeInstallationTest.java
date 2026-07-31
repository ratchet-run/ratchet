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

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.PayloadSerializerHolder;

class PayloadSerializerRuntimeInstallationTest {

  @AfterEach
  void resetHolder() {
    PayloadSerializerHolder.set(null);
  }

  @Test
  void installsAndUninstallsSingleSerializer() {
    PayloadSerializer serializer = new StubSerializer();
    PayloadSerializerRuntimeInstallation installation =
        new PayloadSerializerRuntimeInstallation(List.of(serializer));
    Object owner = new Object();

    installation.install(owner);
    assertSame(serializer, PayloadSerializerHolder.get());

    installation.uninstall(owner);
    assertNotSame(serializer, PayloadSerializerHolder.get());
  }

  @Test
  void ambiguousSerializersUseFallback() {
    PayloadSerializer first = new StubSerializer();
    PayloadSerializerRuntimeInstallation installation =
        new PayloadSerializerRuntimeInstallation(List.of(first, new StubSerializer()));
    Object owner = new Object();

    installation.install(owner);

    assertNotSame(first, PayloadSerializerHolder.get());
    installation.uninstall(owner);
  }

  private static final class StubSerializer implements PayloadSerializer {

    @Override
    public String serialize(Object payload) {
      return String.valueOf(payload);
    }

    @Override
    public <T> T deserialize(String json, Class<T> type) {
      return null;
    }
  }
}
