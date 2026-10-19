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
package run.ratchet.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.inject.BeanDefinition;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.SchedulerLifecycleHook;

class MicronautLifecycleHooksTest {
  @Test
  void partialAcquisitionClosesAllHandlesAndPreservesOriginalFailure() {
    BeanContext context = mock(BeanContext.class);
    BeanDefinition<SchedulerLifecycleHook> first = prototype();
    BeanDefinition<SchedulerLifecycleHook> second = prototype();
    BeanDefinition<SchedulerLifecycleHook> broken = prototype();
    var one = new EqualHook();
    var two = new EqualHook();
    var original = new IllegalStateException("acquire");
    var cleanup = new AssertionError("destroy");
    when(context.getBeanDefinitions(SchedulerLifecycleHook.class))
        .thenReturn(List.of(first, second, broken));
    var oneRegistration = registration(context, first, one);
    var twoRegistration = registration(context, second, two);
    when(context.getBeanRegistration(broken)).thenThrow(original);
    doThrow(cleanup).when(context).destroyBean(same(oneRegistration));
    var hooks = new MicronautLifecycleHooks(context);
    assertSame(original, assertThrows(IllegalStateException.class, hooks::acquire));
    assertArrayEquals(new Throwable[] {cleanup}, original.getSuppressed());
    verify(context).destroyBean(same(oneRegistration));
    verify(context).destroyBean(same(twoRegistration));
    hooks.release(one);
    hooks.release(two);
    verify(context, times(1)).destroyBean(same(oneRegistration));
  }

  @Test
  void identityOwnershipReleasesPrototypesOnceAndBorrowsSingletons() {
    BeanContext context = mock(BeanContext.class);
    var first = prototype();
    var second = prototype();
    var singleton = prototype();
    when(singleton.getScope()).thenReturn(Optional.empty());
    var one = new EqualHook();
    var two = new EqualHook();
    var shared = new EqualHook();
    when(context.getBeanDefinitions(SchedulerLifecycleHook.class))
        .thenReturn(List.of(first, second, singleton));
    var oneRegistration = registration(context, first, one);
    var twoRegistration = registration(context, second, two);
    var sharedRegistration = registration(context, singleton, shared);
    var hooks = new MicronautLifecycleHooks(context);
    assertEquals(3, hooks.acquire().size());
    hooks.release(one);
    hooks.release(one);
    hooks.release(two);
    hooks.release(shared);
    verify(context, times(1)).destroyBean(same(oneRegistration));
    verify(context, times(1)).destroyBean(same(twoRegistration));
    verify(context, never()).destroyBean(same(sharedRegistration));
  }

  @SuppressWarnings("unchecked")
  private BeanRegistration<SchedulerLifecycleHook> registration(
      BeanContext context,
      BeanDefinition<SchedulerLifecycleHook> definition,
      SchedulerLifecycleHook bean) {
    BeanRegistration<SchedulerLifecycleHook> registration = mock(BeanRegistration.class);
    when(registration.getBean()).thenReturn(bean);
    when(context.getBeanRegistration(definition)).thenReturn(registration);
    return registration;
  }

  @SuppressWarnings("unchecked")
  private BeanDefinition<SchedulerLifecycleHook> prototype() {
    BeanDefinition<SchedulerLifecycleHook> definition = mock(BeanDefinition.class);
    when(definition.getScope()).thenReturn(Optional.of(Prototype.class));
    return definition;
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
