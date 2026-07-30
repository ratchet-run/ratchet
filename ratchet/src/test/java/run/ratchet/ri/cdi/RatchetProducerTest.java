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

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.PayloadSerializerHolder;

class RatchetProducerTest {

  @AfterEach
  void resetHolder() {
    PayloadSerializerHolder.set(null);
  }

  @Test
  void dependentPayloadSerializerIsDestroyedAtShutdown() {
    PayloadSerializer serializer = mock(PayloadSerializer.class);
    Instance<PayloadSerializer> serializers = mock(Instance.class);
    Instance.Handle<PayloadSerializer> handle = mock(Instance.Handle.class);
    Bean<PayloadSerializer> bean = mock(Bean.class);
    when(serializers.isResolvable()).thenReturn(true);
    when(serializers.getHandle()).thenReturn(handle);
    when(handle.get()).thenReturn(serializer);
    when(handle.getBean()).thenReturn(bean);
    doReturn(Dependent.class).when(bean).getScope();

    RatchetProducer producer = new RatchetProducer();
    Object ownerToken = new Object();
    producer.payloadSerializerInstallation(serializers).install(ownerToken);

    assertSame(serializer, PayloadSerializerHolder.get());

    producer.unregisterPayloadSerializer();

    verify(handle).destroy();
    assertNotSame(serializer, PayloadSerializerHolder.get());
  }
}
