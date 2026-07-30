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
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import run.ratchet.spi.BeanResolver;

/**
 * Resolves CDI beans by type via {@link Instance}. Throws {@link IllegalStateException} if no bean
 * or multiple beans are found. Direct resolution retains the legacy rejection of {@link
 * Dependent}-scoped beans; managed resolution destroys dependent handles when they close.
 */
@ApplicationScoped
public class CdiBeanResolver implements BeanResolver {

  private final Instance<Object> allBeans;

  protected CdiBeanResolver() {
    this.allBeans = null;
  }

  @Inject
  public CdiBeanResolver(@Any Instance<Object> allBeans) {
    this.allBeans = allBeans;
  }

  @Override
  public <T> T resolve(Class<T> type) {
    Instance<T> instance = selectResolvable(type);
    Instance.Handle<T> handle = instance.getHandle();
    if (handle.getBean().getScope().equals(Dependent.class)) {
      throw new IllegalStateException(
          "Cannot resolve @Dependent-scoped bean for type: "
              + type.getName()
              + ". BeanResolver does not manage the lifecycle of @Dependent beans."
              + " Inject the bean directly or use a wider scope.");
    }
    return handle.get();
  }

  @Override
  public <T> ManagedBeanHandle<T> resolveManaged(Class<T> type) {
    Instance.Handle<T> handle = selectResolvable(type).getHandle();
    boolean dependent = handle.getBean().getScope().equals(Dependent.class);
    return new ManagedBeanHandle<>() {
      @Override
      public T get() {
        return handle.get();
      }

      @Override
      public void close() {
        if (dependent) {
          handle.destroy();
        }
      }
    };
  }

  private <T> Instance<T> selectResolvable(Class<T> type) {
    Instance<T> instance = allBeans.select(type);
    if (instance.isUnsatisfied()) {
      throw new IllegalStateException("No CDI bean found for type: " + type.getName());
    }
    if (instance.isAmbiguous()) {
      throw new IllegalStateException(
          "Multiple CDI beans found for type: "
              + type.getName()
              + ". Use a qualifier to disambiguate.");
    }
    return instance;
  }
}
