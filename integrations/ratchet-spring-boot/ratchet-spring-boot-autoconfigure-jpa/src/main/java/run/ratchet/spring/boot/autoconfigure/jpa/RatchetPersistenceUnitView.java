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
package run.ratchet.spring.boot.autoconfigure.jpa;

import jakarta.persistence.spi.PersistenceUnitInfo;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.List;
import org.springframework.orm.jpa.persistenceunit.SmartPersistenceUnitInfo;

/** Overrides only Ratchet's four metadata values, forwarding the runtime's complete interface. */
final class RatchetPersistenceUnitView {
  private RatchetPersistenceUnitView() {}

  static PersistenceUnitInfo create(
      PersistenceUnitInfo original,
      List<String> classes,
      List<String> mappings,
      URL root,
      List<URL> archives) {
    Class<?>[] interfaces =
        original instanceof SmartPersistenceUnitInfo
            ? new Class<?>[] {PersistenceUnitInfo.class, SmartPersistenceUnitInfo.class}
            : new Class<?>[] {PersistenceUnitInfo.class};
    return (PersistenceUnitInfo)
        Proxy.newProxyInstance(
            RatchetPersistenceUnitView.class.getClassLoader(),
            interfaces,
            (proxy, method, args) -> {
              if (method.getParameterCount() == 0) {
                switch (method.getName()) {
                  case "getManagedClassNames":
                    return classes;
                  case "getMappingFileNames":
                    return mappings;
                  case "getPersistenceUnitRootUrl":
                    return root;
                  case "getJarFileUrls":
                    return archives;
                }
              }
              return RatchetProxyInvocation.forward(proxy, original, method, args);
            });
  }
}
