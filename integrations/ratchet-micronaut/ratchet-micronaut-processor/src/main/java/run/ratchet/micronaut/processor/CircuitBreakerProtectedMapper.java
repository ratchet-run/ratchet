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
package run.ratchet.micronaut.processor;

import io.micronaut.aop.Around;
import io.micronaut.aop.InterceptorBinding;
import io.micronaut.aop.InterceptorBindingDefinitions;
import io.micronaut.aop.InterceptorKind;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.inject.annotation.NamedAnnotationMapper;
import io.micronaut.inject.visitor.VisitorContext;
import java.lang.annotation.Annotation;
import java.util.List;

/** Adds compile-time AOP advice to the portable Ratchet circuit breaker annotation. */
public final class CircuitBreakerProtectedMapper implements NamedAnnotationMapper {
  @Override
  public String getName() {
    return "run.ratchet.api.CircuitBreakerProtected";
  }

  @Override
  public List<AnnotationValue<?>> map(
      AnnotationValue<Annotation> annotation, VisitorContext visitorContext) {
    // Emit the binding as a top-level InterceptorBindingDefinitions container, the form Micronaut's
    // own InterceptorBeanMapper produces. A binding nested as a stereotype of @Around is ignored,
    // which leaves the generated proxy with no interceptors.
    AnnotationValue<?> binding =
        AnnotationValue.builder(InterceptorBinding.class)
            .value(getName())
            .member("kind", InterceptorKind.AROUND)
            .build();
    // Micronaut does not carry class-level advice from a superclass down to a subclass, so a
    // subclass of an annotated class must repeat the annotation.
    return List.of(
        AnnotationValue.builder(Around.class).build(),
        AnnotationValue.builder(InterceptorBindingDefinitions.class).values(binding).build());
  }
}
