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
package run.ratchet.spring.boot.it.sharedtck.fixture.tck;

import java.util.Iterator;
import java.util.ServiceLoader;

/** Loads the consuming store module's one shared-TCK binding. */
public final class StoreTckBindings {

  private StoreTckBindings() {}

  public static StoreTckBinding binding() {
    return BindingHolder.BINDING;
  }

  private static StoreTckBinding loadBinding() {
    Iterator<StoreTckBinding> bindings = ServiceLoader.load(StoreTckBinding.class).iterator();
    if (!bindings.hasNext()) {
      throw new IllegalStateException(
          "No StoreTckBinding provider is registered on the test classpath");
    }
    StoreTckBinding binding = bindings.next();
    if (bindings.hasNext()) {
      throw new IllegalStateException(
          "Multiple StoreTckBinding providers are registered on the test classpath");
    }
    return binding;
  }

  private static final class BindingHolder {

    private static final StoreTckBinding BINDING = loadBinding();
  }
}
