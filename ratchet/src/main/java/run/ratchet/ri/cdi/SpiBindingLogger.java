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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.AmbiguousResolutionException;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadEncryption;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/**
 * Logs, at application startup, which implementation actually won CDI resolution for every
 * application-overridable Ratchet SPI seam.
 *
 * <p>An application {@code @Alternative @Priority(APPLICATION)} bean can silently fail to apply
 * (for example a sibling EAR module the RI's classloader cannot see), and today there is no way to
 * tell which implementation is actually active short of attaching a debugger. This bean prints one
 * INFO line naming the resolved class per seam, turning that mis-selection into a log read instead
 * of a multi-week investigation.
 *
 * <p>Resolution problems are caught and recorded as a state string rather than thrown — a
 * diagnostic must never itself break startup.
 */
@ApplicationScoped
public class SpiBindingLogger {

  private static final Logger log = Logger.getLogger(SpiBindingLogger.class);

  private static final String UNSATISFIED = "<none>";
  private static final String AMBIGUOUS = "<ambiguous>";
  private static final String ERROR = "<error>";

  /**
   * Seams resolved to a single winner. {@link PayloadEncryption} is reported separately below since
   * several engines may legitimately be installed at once (algorithm rotation), so "the resolved
   * bean" is not a meaningful question for it. {@code LambdaAnalyzer} is deliberately excluded:
   * {@code AsmLambdaAnalyzer} is invoked as a static utility ({@code
   * AsmLambdaAnalyzer.inspect(...)}), never through CDI resolution, so it has no binding to report.
   */
  private static final List<Class<?>> SINGLE_RESOLUTION_SEAMS =
      List.of(
          CallerPrincipalProvider.class,
          JobAuthorizationPolicy.class,
          ClassPolicy.class,
          RatchetEntityManagerProvider.class,
          PayloadSerializer.class,
          ResilienceStrategy.class,
          ErrorSanitizer.class,
          MetricsCollector.class,
          TracingCollector.class,
          NodeIdentityProvider.class,
          ExecutorProvider.class,
          RetryPolicy.class);

  private final BeanManager beanManager;

  /**
   * No-arg constructor so Weld can instantiate the client-proxy subclass (CDI 4.0 §3.15); never
   * used for a real instance.
   */
  protected SpiBindingLogger() {
    this.beanManager = null;
  }

  @Inject
  public SpiBindingLogger(BeanManager beanManager) {
    this.beanManager = beanManager;
  }

  void logBindings(@Observes @Initialized(ApplicationScoped.class) Object event) {
    if (beanManager == null) {
      return;
    }
    StringBuilder message = new StringBuilder("Ratchet SPI bindings: ");
    for (int i = 0; i < SINGLE_RESOLUTION_SEAMS.size(); i++) {
      if (i > 0) {
        message.append(", ");
      }
      Class<?> seam = SINGLE_RESOLUTION_SEAMS.get(i);
      message.append(seam.getSimpleName()).append('=').append(resolveBinding(seam));
    }
    message.append(", PayloadEncryption=").append(resolveEncryptionEngines());
    log.info(message.toString());
  }

  /**
   * Resolves the single bean CDI would hand out for {@code seamType} without instantiating it.
   * {@link BeanManager#getBeans(java.lang.reflect.Type, java.lang.annotation.Annotation...)} with
   * no qualifiers implies {@code @Default}, which matches every seam listed above.
   */
  private String resolveBinding(Class<?> seamType) {
    try {
      Set<Bean<?>> beans = beanManager.getBeans(seamType);
      if (beans.isEmpty()) {
        return UNSATISFIED;
      }
      Bean<?> winner = beanManager.resolve(beans);
      return winner.getBeanClass().getName();
    } catch (AmbiguousResolutionException e) {
      return AMBIGUOUS;
    } catch (RuntimeException e) {
      log.warnf(e, "Failed to resolve SPI binding for %s", seamType.getName());
      return ERROR;
    }
  }

  /**
   * {@link PayloadEncryption} is a multi-engine seam by design (an old algorithm stays installed to
   * decrypt not-yet-drained rows during rotation), so every installed engine is reported rather
   * than a single resolved winner.
   */
  private String resolveEncryptionEngines() {
    try {
      Set<Bean<?>> beans = beanManager.getBeans(PayloadEncryption.class);
      if (beans.isEmpty()) {
        return UNSATISFIED;
      }
      return beans.stream()
          .map(bean -> bean.getBeanClass().getName())
          .sorted()
          .collect(Collectors.joining("+"));
    } catch (RuntimeException e) {
      log.warnf(e, "Failed to enumerate PayloadEncryption engines");
      return ERROR;
    }
  }
}
