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
package run.ratchet.spring.boot.autoconfigure.internal.jpa;

import jakarta.persistence.spi.PersistenceProvider;
import jakarta.persistence.spi.PersistenceUnitInfo;
import java.lang.reflect.Proxy;
import java.util.function.BiFunction;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

/** Applies the same unit transformation during container creation and schema generation. */
public final class RatchetPersistenceProvider {
  private RatchetPersistenceProvider() {}

  public static void install(
      LocalContainerEntityManagerFactoryBean factory,
      BiFunction<PersistenceProvider, PersistenceUnitInfo, PersistenceUnitInfo> transform,
      boolean required) {
    PersistenceProvider provider = factory.getPersistenceProvider();
    if (provider == null && factory.getJpaVendorAdapter() != null) {
      provider = factory.getJpaVendorAdapter().getPersistenceProvider();
    }
    if (provider == null) {
      if (required)
        throw new IllegalStateException(
            "Ratchet could not locate the PersistenceProvider for the selected"
                + " LocalContainerEntityManagerFactoryBean");
      return;
    }
    PersistenceProvider original = provider;
    factory.setPersistenceProvider(
        (PersistenceProvider)
            Proxy.newProxyInstance(
                RatchetPersistenceProvider.class.getClassLoader(),
                new Class<?>[] {PersistenceProvider.class},
                (proxy, method, args) -> {
                  if ((method.getName().equals("createContainerEntityManagerFactory")
                          || method.getName().equals("generateSchema"))
                      && args != null
                      && args.length > 0
                      && args[0] instanceof PersistenceUnitInfo unit) {
                    args[0] = transform.apply(original, unit);
                  }
                  return RatchetProxyInvocation.forward(proxy, original, method, args);
                }));
  }
}
