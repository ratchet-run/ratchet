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
import io.micronaut.inject.ProxyBeanDefinition;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.ri.cdi.RecurringJobProcessor;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RecurringJobStore;

/**
 * Discovers Micronaut bean metadata, then delegates validation and persistence to the shared
 * engine.
 */
public final class MicronautRecurringDiscovery {
  private final BeanContext beanFactory;
  private final InvocationSubmissionService submissions;
  private final JobStore store;
  private final RecurringAnnotationMaintenanceService maintenance;
  private final RecurringMethodInvoker invoker;
  private final StartupCoordinator startup;
  private final RecurringRegistrationState registration;
  private final RatchetOptions options;
  private final Clock clock;

  public MicronautRecurringDiscovery(
      BeanContext beanFactory,
      InvocationSubmissionService submissions,
      JobStore store,
      RecurringAnnotationMaintenanceService maintenance,
      RecurringMethodInvoker invoker,
      StartupCoordinator startup,
      RecurringRegistrationState registration,
      RatchetOptions options,
      Clock clock) {
    this.beanFactory = beanFactory;
    this.submissions = submissions;
    this.store = store;
    this.maintenance = maintenance;
    this.invoker = invoker;
    this.startup = startup;
    this.registration = registration;
    this.options = options;
    this.clock = clock;
  }

  public void register() {
    var types = discover();
    types.forEach(this::validateExposedMethods);
    var processor =
        new RecurringJobProcessor(
            submissions,
            store,
            maintenance,
            invoker,
            startup,
            registration,
            options,
            types,
            clock,
            store.capability(RecurringJobStore.class).orElse(null),
            MicronautRecurringDiscovery::recurringMethods);
    if (!processor.registerRecurringJobs())
      throw new IllegalStateException(
          "Ratchet recurring registration did not complete; workers have not started");
  }

  private void validateExposedMethods(Class<?> type) {
    new MicronautBeanResolver(beanFactory).validateResolvable(type);
  }

  Set<Class<?>> discover() {
    Set<Class<?>> types = new LinkedHashSet<>();
    for (var definition : beanFactory.getAllBeanDefinitions()) {
      Class<?> type =
          definition instanceof ProxyBeanDefinition<?> proxy
              ? proxy.getTargetType()
              : definition.getBeanType();
      if (type.getName().startsWith("io.micronaut.")) continue;
      if (recurringMethods(type).values().stream().anyMatch(Recurring::enabled)) types.add(type);
    }
    return Set.copyOf(types);
  }

  static Map<Method, Recurring> recurringMethods(Class<?> type) {
    Map<Method, Recurring> methods = new LinkedHashMap<>();
    collectMethods(type, methods, new LinkedHashSet<>());
    return methods;
  }

  private static void collectMethods(
      Class<?> type, Map<Method, Recurring> methods, Set<List<Object>> seen) {
    if (type == null || type == Object.class) return;
    for (Method method : type.getDeclaredMethods()) {
      if (method.isBridge() || method.isSynthetic()) continue;
      List<Object> signature = List.of(method.getName(), Arrays.asList(method.getParameterTypes()));
      if (!seen.add(signature)) continue;
      Recurring annotation = method.getAnnotation(Recurring.class);
      if (annotation == null) annotation = inheritedAnnotation(type, method);
      if (annotation != null) methods.put(method, annotation);
    }
    for (Class<?> parent : type.getInterfaces()) collectMethods(parent, methods, seen);
    collectMethods(type.getSuperclass(), methods, seen);
  }

  private static Recurring inheritedAnnotation(Class<?> type, Method method) {
    for (Class<?> parent : type.getInterfaces()) {
      Recurring annotation = annotationOn(parent, method);
      if (annotation != null) return annotation;
    }
    return annotationOn(type.getSuperclass(), method);
  }

  private static Recurring annotationOn(Class<?> type, Method method) {
    if (type == null || type == Object.class) return null;
    try {
      Recurring annotation =
          type.getDeclaredMethod(method.getName(), method.getParameterTypes())
              .getAnnotation(Recurring.class);
      if (annotation != null) return annotation;
    } catch (NoSuchMethodException ignored) {
    }
    return inheritedAnnotation(type, method);
  }
}
