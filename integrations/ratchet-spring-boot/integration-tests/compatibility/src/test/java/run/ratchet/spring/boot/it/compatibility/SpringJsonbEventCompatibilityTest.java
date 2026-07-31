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
package run.ratchet.spring.boot.it.compatibility;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.util.ClassUtils;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration;

class SpringJsonbEventCompatibilityTest {

  private static final String BOOT_JSONB_AUTO_CONFIGURATION =
      "org.springframework.boot.autoconfigure.jsonb.JsonbAutoConfiguration";

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(RatchetAutoConfiguration.class))
          .withPropertyValues("ratchet.allow-empty-class-policy=true");

  @Test
  void supportedBootLaneBorrowsBootJsonbWhileTheOtherLaneOwnsItsJsonb() {
    boolean bootSuppliesJsonb =
        ClassUtils.isPresent(BOOT_JSONB_AUTO_CONFIGURATION, getClass().getClassLoader());

    laneContextRunner()
        .run(
            context -> {
              PayloadSerializer serializer = context.getBean(PayloadSerializer.class);
              Jsonb serializerJsonb = (Jsonb) field(serializer, "jsonb");
              boolean ownsJsonb = (boolean) field(serializer, "ownsJsonb");

              assertEquals(!bootSuppliesJsonb, ownsJsonb);
              if (bootSuppliesJsonb) {
                assertSame(context.getBean(Jsonb.class), serializerJsonb);
              } else {
                assertEquals(0, context.getBeanNamesForType(Jsonb.class, true, false).length);
              }
            });
  }

  @Test
  void ratchetOwnedJsonbClosesExactlyOnce() throws Exception {
    Jsonb ownedJsonb = mock(Jsonb.class);
    AtomicReference<PayloadSerializer> serializer = new AtomicReference<>();

    try (MockedStatic<JsonbBuilder> jsonbBuilder = mockStatic(JsonbBuilder.class)) {
      jsonbBuilder.when(JsonbBuilder::create).thenReturn(ownedJsonb);

      contextRunner.run(context -> serializer.set(context.getBean(PayloadSerializer.class)));

      verify(ownedJsonb, times(1)).close();
      ((DisposableBean) serializer.get()).destroy();
      verify(ownedJsonb, times(1)).close();
    }
  }

  @Test
  void userJsonbIsBorrowedAndNeverClosedByRatchet() throws Exception {
    Jsonb userJsonb = mock(Jsonb.class);

    contextRunner
        .withBean(
            "userJsonb",
            Jsonb.class,
            () -> userJsonb,
            beanDefinition -> beanDefinition.setDestroyMethodName(""))
        .run(
            context -> {
              PayloadSerializer serializer = context.getBean(PayloadSerializer.class);
              assertSame(userJsonb, field(serializer, "jsonb"));
              assertFalse((boolean) field(serializer, "ownsJsonb"));
            });

    verify(userJsonb, never()).close();
  }

  @Test
  void defaultSerializerRoundTripsPayloadAndUserSerializerBacksOff() {
    contextRunner.run(
        context -> {
          PayloadSerializer serializer = context.getBean(PayloadSerializer.class);
          SamplePayload payload = new SamplePayload("alpha", 3);

          String json = serializer.serialize(payload);

          assertEquals(payload, serializer.deserialize(json, SamplePayload.class));
        });

    PayloadSerializer userSerializer = mock(PayloadSerializer.class);
    contextRunner
        .withBean(PayloadSerializer.class, () -> userSerializer)
        .run(context -> assertSame(userSerializer, context.getBean(PayloadSerializer.class)));
  }

  @Test
  void eventBridgeDeliversOnceSynchronouslyAndClearsTheClosedContext() {
    AtomicReference<Consumer<Object>> bridge = new AtomicReference<>();
    AtomicReference<RecordingListener> listener = new AtomicReference<>();
    Thread publishingThread = Thread.currentThread();

    contextRunner
        .withBean(RecordingListener.class, RecordingListener::new)
        .run(
            context -> {
              RecordingListener recordingListener = context.getBean(RecordingListener.class);
              listener.set(recordingListener);
              @SuppressWarnings("unchecked")
              Consumer<Object> eventBridge =
                  (Consumer<Object>) context.getBean("ratchetSpringEventBridge");
              bridge.set(eventBridge);

              context.getBean(InternalEventPublisher.class).publish(new TestEvent("first"));

              assertEquals(1, recordingListener.deliveries.get());
              assertSame(publishingThread, recordingListener.deliveryThread.get());
              assertEquals(List.of("first"), recordingListener.payloads);
            });

    assertNull(field(bridge.get(), "publisher"));
    assertDoesNotThrow(() -> bridge.get().accept(new TestEvent("after-close")));
    assertEquals(1, listener.get().deliveries.get());
  }

  private ApplicationContextRunner laneContextRunner() {
    List<Class<?>> configurations = new ArrayList<>();
    configurations.add(RatchetAutoConfiguration.class);
    if (ClassUtils.isPresent(BOOT_JSONB_AUTO_CONFIGURATION, getClass().getClassLoader())) {
      try {
        configurations.add(
            ClassUtils.forName(BOOT_JSONB_AUTO_CONFIGURATION, getClass().getClassLoader()));
      } catch (ClassNotFoundException exception) {
        throw new AssertionError("Boot JSON-B auto-configuration disappeared", exception);
      }
    }
    return new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(configurations.toArray(Class<?>[]::new)))
        .withPropertyValues("ratchet.allow-empty-class-policy=true");
  }

  private static Object field(Object target, String name) {
    try {
      Field field = target.getClass().getDeclaredField(name);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException exception) {
      throw new AssertionError("Unable to inspect " + name + " on " + target.getClass(), exception);
    }
  }

  static final class RecordingListener implements ApplicationListener<TestEvent> {

    private final AtomicInteger deliveries = new AtomicInteger();
    private final AtomicReference<Thread> deliveryThread = new AtomicReference<>();
    private final List<String> payloads = new ArrayList<>();

    @Override
    public void onApplicationEvent(TestEvent event) {
      deliveries.incrementAndGet();
      deliveryThread.set(Thread.currentThread());
      payloads.add(event.payload());
    }
  }

  static final class TestEvent extends ApplicationEvent {

    private final String payload;

    TestEvent(String payload) {
      super(payload);
      this.payload = payload;
    }

    String payload() {
      return payload;
    }
  }

  public static final class SamplePayload {

    private String name;
    private int attempt;

    public SamplePayload() {}

    SamplePayload(String name, int attempt) {
      this.name = name;
      this.attempt = attempt;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAttempt() {
      return attempt;
    }

    public void setAttempt(int attempt) {
      this.attempt = attempt;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof SamplePayload payload
          && attempt == payload.attempt
          && java.util.Objects.equals(name, payload.name);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(name, attempt);
    }
  }
}
