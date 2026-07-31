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
package run.ratchet.spi;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.Prioritized;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import run.ratchet.api.Incubating;

/**
 * Internal utility for Ratchet modules that adapt CDI {@link Instance}-backed principal sources.
 *
 * <p>This type is public only so Ratchet modules in different artifacts can share identical lookup
 * semantics. It is not an application SPI; applications should implement {@link PrincipalSource} or
 * {@link CallerPrincipalResolver} instead.
 */
@Incubating
public final class PrincipalSourceInstances {

  private PrincipalSourceInstances() {}

  /**
   * Resolves a principal from a CDI {@link Instance}, destroying dependent handles after use and
   * converting lookup failures to {@link Optional#empty()} after the caller logs them.
   */
  public static <T> Optional<String> currentPrincipal(
      Instance<T> instances,
      Function<T, Optional<String>> extractor,
      String missingInstanceMessage,
      Consumer<RuntimeException> lookupFailureLogger) {
    if (instances == null) {
      throw new IllegalStateException(missingInstanceMessage);
    }
    try {
      Iterable<? extends Instance.Handle<T>> handles = instances.handles();
      if (handles == null) {
        return Optional.empty();
      }
      List<Instance.Handle<T>> orderedHandles = new ArrayList<>();
      handles.forEach(orderedHandles::add);
      orderedHandles.sort(
          Comparator.comparingInt(
                  (Instance.Handle<T> handle) -> priority(handle, lookupFailureLogger))
              .reversed()
              .thenComparing(handle -> beanClassName(handle, lookupFailureLogger)));
      return firstNonEmpty(
          orderedHandles, handle -> principalFromHandle(handle, extractor), lookupFailureLogger);
    } catch (RuntimeException e) {
      lookupFailureLogger.accept(e);
      return Optional.empty();
    }
  }

  /**
   * Resolves the first non-empty principal from an ordered collection of sources. A null source,
   * null result, or runtime failure degrades only that source to empty and resolution continues.
   */
  public static <T> Optional<String> currentPrincipal(
      Iterable<T> sources,
      Function<T, Optional<String>> extractor,
      Consumer<RuntimeException> lookupFailureLogger) {
    Iterable<T> orderedSources = sources == null ? Collections.emptyList() : sources;
    return firstNonEmpty(orderedSources, extractor, lookupFailureLogger);
  }

  private static <S> Optional<String> firstNonEmpty(
      Iterable<? extends S> sources,
      Function<S, Optional<String>> extractor,
      Consumer<RuntimeException> lookupFailureLogger) {
    for (S source : sources) {
      try {
        Optional<String> principal = extractor.apply(source);
        if (principal != null) {
          Optional<String> nonEmpty = principal.filter(name -> !name.isEmpty());
          if (nonEmpty.isPresent()) {
            return nonEmpty;
          }
        }
      } catch (RuntimeException e) {
        lookupFailureLogger.accept(e);
      }
    }
    return Optional.empty();
  }

  private static <T> Optional<String> principalFromHandle(
      Instance.Handle<T> handle, Function<T, Optional<String>> extractor) {
    try {
      return handle == null ? Optional.empty() : extractor.apply(handle.get());
    } finally {
      if (handle != null && handle.getBean().getScope().equals(Dependent.class)) {
        handle.destroy();
      }
    }
  }

  private static int priority(
      Instance.Handle<?> handle, Consumer<RuntimeException> lookupFailureLogger) {
    try {
      Bean<?> bean = handle == null ? null : handle.getBean();
      if (bean instanceof Prioritized prioritized) {
        return prioritized.getPriority();
      }
      Class<?> beanClass = bean == null ? null : bean.getBeanClass();
      Priority annotation = beanClass == null ? null : beanClass.getAnnotation(Priority.class);
      return annotation == null ? 0 : annotation.value();
    } catch (RuntimeException e) {
      lookupFailureLogger.accept(e);
      return 0;
    }
  }

  private static String beanClassName(
      Instance.Handle<?> handle, Consumer<RuntimeException> lookupFailureLogger) {
    try {
      Bean<?> bean = handle == null ? null : handle.getBean();
      Class<?> beanClass = bean == null ? null : bean.getBeanClass();
      return beanClass == null ? "\uffff" : beanClass.getName();
    } catch (RuntimeException e) {
      lookupFailureLogger.accept(e);
      return "\uffff";
    }
  }
}
