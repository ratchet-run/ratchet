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

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
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
