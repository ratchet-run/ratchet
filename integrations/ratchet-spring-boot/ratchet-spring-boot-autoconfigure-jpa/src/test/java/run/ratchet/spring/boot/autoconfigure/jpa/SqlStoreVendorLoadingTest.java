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

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Locale;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class SqlStoreVendorLoadingTest {
  @ParameterizedTest
  @EnumSource(SqlStoreVendor.class)
  void otherVendorPackagesAreOptional(SqlStoreVendor vendor) throws Exception {
    try (URLClassLoader loader = isolated(vendor, false)) {
      Object loaded = selected(loader, vendor);
      assertThat(invoke(loaded, "storeType")).isEqualTo(vendor.storeType());
      assertThat(invoke(loaded, "migrationDialect").getClass())
          .isEqualTo(vendor.migrationDialect().getClass());
      assertThat(invoke(loaded, "jpaMappingFile")).isEqualTo(vendor.jpaMappingFile());
    }
  }

  @ParameterizedTest
  @EnumSource(SqlStoreVendor.class)
  void missingSelectedVendorHasActionableDiagnostic(SqlStoreVendor vendor) throws Exception {
    try (URLClassLoader loader = isolated(vendor, true)) {
      Object loaded = selected(loader, vendor);
      for (String method : new String[] {"storeType", "migrationDialect"}) {
        assertThatThrownBy(() -> invoke(loaded, method))
            .isInstanceOf(InvocationTargetException.class)
            .cause()
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining(
                "Add run.ratchet:ratchet-store-" + vendor.name().toLowerCase(Locale.ROOT));
      }
    }
  }

  private static Object selected(ClassLoader loader, SqlStoreVendor vendor) throws Exception {
    for (Object value : loader.loadClass(SqlStoreVendor.class.getName()).getEnumConstants()) {
      if (value.toString().equals(vendor.name())) return value;
    }
    throw new AssertionError(vendor);
  }

  private static Object invoke(Object target, String name) throws Exception {
    Method method = target.getClass().getDeclaredMethod(name);
    method.setAccessible(true);
    return method.invoke(target);
  }

  private static URLClassLoader isolated(SqlStoreVendor selected, boolean missingSelected) {
    URL source = SqlStoreVendor.class.getProtectionDomain().getCodeSource().getLocation();
    return new URLClassLoader(new URL[] {source}, SqlStoreVendor.class.getClassLoader()) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
          if (name.startsWith("org.hibernate.")) throw new ClassNotFoundException(name);
          for (SqlStoreVendor vendor : SqlStoreVendor.values()) {
            if ((missingSelected || vendor != selected)
                && name.startsWith(
                    "run.ratchet.store." + vendor.name().toLowerCase(Locale.ROOT) + ".")) {
              throw new ClassNotFoundException(name);
            }
          }
          if (name.startsWith(SqlStoreVendor.class.getName())) {
            Class<?> type = findLoadedClass(name);
            if (type == null) type = findClass(name);
            if (resolve) resolveClass(type);
            return type;
          }
          return super.loadClass(name, resolve);
        }
      }
    };
  }
}
