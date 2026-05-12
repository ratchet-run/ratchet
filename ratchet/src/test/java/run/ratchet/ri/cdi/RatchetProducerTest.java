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
    producer.registerPayloadSerializer(new Object(), serializers);

    assertSame(serializer, PayloadSerializerHolder.get());

    producer.unregisterPayloadSerializer();

    verify(handle).destroy();
    assertNotSame(serializer, PayloadSerializerHolder.get());
  }
}
