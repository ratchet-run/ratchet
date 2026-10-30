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

/** Detects optional observability without linking the core factory to Micrometer types. */
final class MicronautObservabilityConditions {
  private MicronautObservabilityConditions() {}

  private static boolean available(BeanContext context, String adapter, String bean) {
    try {
      ClassLoader loader = context.getClassLoader();
      Class.forName(adapter, false, loader);
      return context.containsBean(Class.forName(bean, false, loader));
    } catch (ClassNotFoundException | LinkageError unavailable) {
      return false;
    }
  }

  static boolean metricsAvailable(BeanContext context) {
    return available(
        context,
        "run.ratchet.micrometer.MicrometerMetricsCollector",
        "io.micrometer.core.instrument.MeterRegistry");
  }

  static boolean tracingAvailable(BeanContext context) {
    return available(
        context,
        "run.ratchet.micrometer.MicrometerTracingCollector",
        "io.micrometer.tracing.Tracer");
  }
}
