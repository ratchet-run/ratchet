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
package run.ratchet.micronaut.jpa.graal;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import run.ratchet.store.schema.RatchetJpaModel;

class RatchetJpaNativeFeatureTest {
  @Test
  void allRatchetMetadataNamesResolveWithoutInitializingClasses() throws Exception {
    var names = new ArrayList<>(RatchetJpaModel.ENTITY_CLASS_NAMES);
    names.addAll(RatchetJpaModel.CONVERTER_CLASS_NAMES);
    names.addAll(RatchetJpaNativeFeature.EXTRA_MODEL_TYPES);
    for (var store : RatchetJpaNativeFeature.STORES) {
      names.add(store.interfaceName());
      names.add(store.implementationName());
      names.add(store.adapterName());
      assertTrue(load(store.interfaceName()).isInterface());
      assertNotNull(load(store.adapterName()).getDeclaredConstructor());
    }
    for (String name : names) assertNotNull(load(name), name);
  }

  private static Class<?> load(String name) throws ClassNotFoundException {
    return Class.forName(name, false, RatchetJpaNativeFeatureTest.class.getClassLoader());
  }
}
