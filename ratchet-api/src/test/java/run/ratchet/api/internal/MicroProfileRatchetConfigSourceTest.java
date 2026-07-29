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
package run.ratchet.api.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.RatchetConfigSource;

class MicroProfileRatchetConfigSourceTest {

  @AfterEach
  void clearConfig() {
    ConfigProvider.clear();
  }

  @Test
  void resolvesInterfaceMethodAndInvokesItOnProviderImplementation() throws Exception {
    ConfigProvider.setValue("ratchet.schema.auto-migrate", "true");

    RatchetConfigSource source = MicroProfileRatchetConfigSource.create().orElseThrow();

    assertEquals(
        Optional.of("true"),
        source.get("ratchet.schema.auto-migrate", "RATCHET_SCHEMA_AUTO_MIGRATE"));

    Field methodField = MicroProfileRatchetConfigSource.class.getDeclaredField("getOptionalValue");
    methodField.setAccessible(true);
    Method getOptionalValue = (Method) methodField.get(source);
    assertSame(Config.class, getOptionalValue.getDeclaringClass());
  }
}
