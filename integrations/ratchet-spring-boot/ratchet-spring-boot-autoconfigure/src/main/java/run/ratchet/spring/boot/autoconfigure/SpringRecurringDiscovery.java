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

import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.Set;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ReflectionUtils;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.ri.cdi.RecurringJobProcessor;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.internal.ManagedInvocation;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RecurringJobStore;

/**
 * Discovers Spring bean metadata, then delegates validation and persistence to the shared engine.
 */
public final class SpringRecurringDiscovery {
  private final ConfigurableListableBeanFactory beanFactory;
  private final InvocationSubmissionService submissions;
  private final JobStore store;
  private final RecurringAnnotationMaintenanceService maintenance;
  private final RecurringMethodInvoker invoker;
  private final StartupCoordinator startup;
  private final RecurringRegistrationState registration;
  private final RatchetOptions options;
  private final Clock clock;

  public SpringRecurringDiscovery(
      ConfigurableListableBeanFactory beanFactory,
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
            store.capability(RecurringJobStore.class).orElse(null));
    if (!processor.registerRecurringJobs())
      throw new IllegalStateException(
          "Ratchet recurring registration did not complete; workers have not started");
  }

  Set<Class<?>> discover() {
    Set<Class<?>> types = new LinkedHashSet<>();
    var resolver = new SpringBeanResolver(beanFactory);
    for (String name : beanFactory.getBeanDefinitionNames()) {
      if (name.startsWith("scopedTarget.")) continue;
      Class<?> type = resolver.targetType(name);
      if (type == null || type.getName().startsWith("org.springframework.")) continue;
      ReflectionUtils.doWithMethods(
          type,
          method -> {
            Recurring annotation =
                AnnotatedElementUtils.findMergedAnnotation(method, Recurring.class);
            if (annotation == null || !annotation.enabled()) return;
            try (var handle = beanFactory.getBean(BeanResolver.class).acquire(type)) {
              ManagedInvocation.exposedMethod(method, handle.instance());
            } catch (NoSuchMethodException failure) {
              throw new IllegalStateException(
                  "Ratchet cannot invoke @Recurring method through its Spring proxy: " + method,
                  failure);
            }
            types.add(type);
          });
    }
    return Set.copyOf(types);
  }
}
