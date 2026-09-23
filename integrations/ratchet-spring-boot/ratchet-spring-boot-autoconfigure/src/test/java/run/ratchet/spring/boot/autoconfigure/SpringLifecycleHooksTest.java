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
package run.ratchet.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import run.ratchet.spi.SchedulerLifecycleHook;

class SpringLifecycleHooksTest {
  @Test
  void partialAcquisitionClosesEveryHandleAndKeepsOriginalFailure() {
    ConfigurableListableBeanFactory factory = mock(ConfigurableListableBeanFactory.class);
    SchedulerLifecycleHook first = new SchedulerLifecycleHook() {};
    SchedulerLifecycleHook second = new SchedulerLifecycleHook() {};
    IllegalStateException original = new IllegalStateException("acquire");
    AssertionError cleanup = new AssertionError("destroy");
    when(factory.getBeanNamesForType(SchedulerLifecycleHook.class))
        .thenReturn(new String[] {"first", "second", "broken"});
    when(factory.getBean("first")).thenReturn(first);
    when(factory.getBean("second")).thenReturn(second);
    when(factory.containsBeanDefinition("first")).thenReturn(true);
    when(factory.containsBeanDefinition("second")).thenReturn(true);
    when(factory.isPrototype("first")).thenReturn(true);
    when(factory.isPrototype("second")).thenReturn(true);
    when(factory.getBean("broken")).thenThrow(original);
    doThrow(cleanup).when(factory).destroyBean("first", first);
    SpringLifecycleHooks hooks = new SpringLifecycleHooks(factory);

    assertThatThrownBy(hooks::acquire).isSameAs(original).hasSuppressedException(cleanup);
    verify(factory).destroyBean("first", first);
    verify(factory).destroyBean("second", second);
    hooks.release(first);
    hooks.release(second);
    verify(factory, times(1)).destroyBean("first", first);
    verify(factory, times(1)).destroyBean("second", second);
  }

  @Test
  void identityOwnershipClosesPrototypesOnceAndLeavesSingletonsToSpring() {
    ConfigurableListableBeanFactory factory = mock(ConfigurableListableBeanFactory.class);
    SchedulerLifecycleHook first = new EqualHook();
    SchedulerLifecycleHook second = new EqualHook();
    SchedulerLifecycleHook singleton = new EqualHook();
    when(factory.getBeanNamesForType(SchedulerLifecycleHook.class))
        .thenReturn(new String[] {"first", "second", "singleton"});
    when(factory.getBean("first")).thenReturn(first);
    when(factory.getBean("second")).thenReturn(second);
    when(factory.getBean("singleton")).thenReturn(singleton);
    when(factory.containsBeanDefinition("first")).thenReturn(true);
    when(factory.containsBeanDefinition("second")).thenReturn(true);
    when(factory.containsBeanDefinition("singleton")).thenReturn(true);
    when(factory.isPrototype("first")).thenReturn(true);
    when(factory.isPrototype("second")).thenReturn(true);
    SpringLifecycleHooks hooks = new SpringLifecycleHooks(factory);
    assertThat(hooks.acquire()).containsExactly(first, second, singleton);
    hooks.release(first);
    verify(factory, never()).destroyBean("second", second);
    hooks.release(first);
    hooks.release(second);
    hooks.release(singleton);
    verify(factory, times(1)).destroyBean("first", first);
    verify(factory, times(1)).destroyBean("second", second);
    verify(factory, never()).destroyBean("singleton", singleton);
  }

  @Test
  void nonIndependentSmartFactoryHookRemainsFactoryOwned() {
    try (var context = new AnnotationConfigApplicationContext()) {
      AtomicInteger destructions = new AtomicInteger();
      PooledHookFactory factory = new PooledHookFactory(destructions);
      context.registerBean("pooledHook", PooledHookFactory.class, () -> factory);
      context.refresh();

      assertThat(context.getBeanFactory().isPrototype("pooledHook")).isTrue();
      SpringLifecycleHooks hooks = new SpringLifecycleHooks(context.getBeanFactory());
      assertThat(hooks.acquire()).containsExactly(factory.hook);
      hooks.release(factory.hook);
      assertThat(destructions).hasValue(0);
    }
  }

  @Test
  void manuallyRegisteredSingletonHookIsBorrowed() {
    try (var context = new AnnotationConfigApplicationContext()) {
      SchedulerLifecycleHook hook = new EqualHook();
      context.getBeanFactory().registerSingleton("manualHook", hook);
      context.refresh();

      SpringLifecycleHooks hooks = new SpringLifecycleHooks(context.getBeanFactory());
      assertThat(hooks.acquire()).containsExactly(hook);
      hooks.release(hook);
    }
  }

  private static final class PooledHookFactory implements SmartFactoryBean<PooledHook> {
    private final PooledHook hook;

    PooledHookFactory(AtomicInteger destructions) {
      hook = new PooledHook(destructions);
    }

    @Override
    public PooledHook getObject() {
      return hook;
    }

    @Override
    public Class<?> getObjectType() {
      return PooledHook.class;
    }

    @Override
    public boolean isSingleton() {
      return false;
    }

    @Override
    public boolean isPrototype() {
      return false;
    }
  }

  private static final class PooledHook implements SchedulerLifecycleHook {
    private final AtomicInteger destructions;

    PooledHook(AtomicInteger destructions) {
      this.destructions = destructions;
    }

    @PreDestroy
    void close() {
      destructions.incrementAndGet();
    }
  }

  private static final class EqualHook implements SchedulerLifecycleHook {
    @Override
    public boolean equals(Object other) {
      return other instanceof EqualHook;
    }

    @Override
    public int hashCode() {
      return 1;
    }
  }
}
