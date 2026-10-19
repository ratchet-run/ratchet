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

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Secondary;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import run.ratchet.spi.BeanResolver.ManagedBean;
import run.ratchet.spi.SchedulerLifecycleHook;

/** Owns hook handles for the runtime, which controls their ordering and release. */
final class MicronautLifecycleHooks {
  private final BeanContext beanFactory;
  private final Map<SchedulerLifecycleHook, List<ManagedBean>> handles = new IdentityHashMap<>();

  MicronautLifecycleHooks(BeanContext beanFactory) {
    this.beanFactory = beanFactory;
  }

  List<SchedulerLifecycleHook> acquire() {
    List<ManagedBean> acquired = new ArrayList<>();
    List<SchedulerLifecycleHook> hooks = new ArrayList<>();
    try {
      for (var definition : beanFactory.getBeanDefinitions(SchedulerLifecycleHook.class)) {
        // Collection lookups include secondary defaults. Retain only the selected product
        // when an application overrides a hook type, without dropping unrelated user hooks.
        if (definition.hasDeclaredStereotype(Secondary.class)
            && !definition.equals(
                beanFactory.getBeanDefinition(
                    definition.getBeanType(), definition.getDeclaredQualifier()))) continue;
        ManagedBean handle = MicronautManagedBeans.acquire(beanFactory, definition);
        acquired.add(handle);
        SchedulerLifecycleHook hook = (SchedulerLifecycleHook) handle.instance();
        handles.computeIfAbsent(hook, ignored -> new ArrayList<>()).add(handle);
        hooks.add(hook);
      }
      return hooks;
    } catch (RuntimeException | Error failure) {
      handles.clear();
      // close() adds cleanup failures to the same exception that is thrown below.
      //noinspection ThrowableNotThrown
      close(acquired, failure);
      throw failure;
    }
  }

  void release(SchedulerLifecycleHook hook) {
    List<ManagedBean> owned = handles.remove(hook);
    if (owned == null) return;
    Throwable failure = close(owned, null);
    if (failure instanceof RuntimeException exception) throw exception;
    if (failure instanceof Error error) throw error;
  }

  private static Throwable close(List<ManagedBean> handles, Throwable failure) {
    for (ManagedBean handle : handles) {
      try {
        handle.close();
      } catch (RuntimeException | Error cleanupFailure) {
        if (failure == null) failure = cleanupFailure;
        else if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
      }
    }
    return failure;
  }
}
