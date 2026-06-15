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
package run.ratchet.micrometer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Singleton;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class MicrometerMeterRegistryProducerTest {

  @Test
  void defaultRegistryProducerDeclaresSingletonScope() throws NoSuchMethodException {
    // @Singleton (not @ApplicationScoped) so Weld doesn't try to proxy abstract MeterRegistry
    // (WELD-001435). Same instance semantics, no proxy required.
    Method method = MicrometerMeterRegistryProducer.class.getMethod("defaultRegistry");

    assertNotNull(method.getAnnotation(Singleton.class));
  }

  @Test
  void defaultRegistryReturnsSharedRegistryInstance() {
    MicrometerMeterRegistryProducer producer = new MicrometerMeterRegistryProducer();

    MeterRegistry first = producer.defaultRegistry();
    MeterRegistry second = producer.defaultRegistry();

    assertSame(first, second);
  }
}
