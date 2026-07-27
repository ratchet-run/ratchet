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
package run.ratchet.quarkus.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import run.ratchet.quarkus.runtime.QuarkusRatchetEntityManagerProvider;

/** Hibernate-backed SQL flavor wiring for the existing {@code ratchet-quarkus} artifact. */
class RatchetSqlProcessor {

  /** Registers the named-ratchet-unit EntityManager provider only for the SQL flavor. */
  @BuildStep
  AdditionalBeanBuildItem entityManagerProvider() {
    return AdditionalBeanBuildItem.builder()
        .addBeanClass(QuarkusRatchetEntityManagerProvider.class)
        .setUnremovable()
        .build();
  }

  /** UuidV7EntityListener is instantiated by Hibernate via reflection. */
  @BuildStep
  void hibernateNativeMetadata(BuildProducer<ReflectiveClassBuildItem> reflective) {
    reflective.produce(
        ReflectiveClassBuildItem.builder("run.ratchet.store.id.UuidV7EntityListener")
            .constructors(true)
            .methods(true)
            .fields(true)
            .build());
  }
}
