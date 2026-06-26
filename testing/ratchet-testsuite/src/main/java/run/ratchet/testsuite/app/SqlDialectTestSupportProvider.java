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
package run.ratchet.testsuite.app;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import run.ratchet.tck.store.SqlDialectTestSupport;

/**
 * Resolves the deployment's single {@link SqlDialectTestSupport} via {@link ServiceLoader}. Each
 * SQL store registers its implementation, and exactly one store is on the WAR classpath, so the
 * lookup is unambiguous. The result is cached because the implementations are stateless.
 */
public final class SqlDialectTestSupportProvider {

  private static final SqlDialectTestSupport INSTANCE = load();

  private SqlDialectTestSupportProvider() {}

  public static SqlDialectTestSupport get() {
    return INSTANCE;
  }

  private static SqlDialectTestSupport load() {
    // The rest of the testsuite assumes exactly one dialect support. Fail loudly on zero (no store
    // active) or more than one (misconfigured profiles / stale classpath) rather than silently
    // picking an arbitrary implementation.
    List<SqlDialectTestSupport> found = new ArrayList<>();
    ServiceLoader.load(SqlDialectTestSupport.class).forEach(found::add);
    if (found.isEmpty()) {
      throw new IllegalStateException(
          "No SqlDialectTestSupport registered on the classpath — the active store module"
              + " must contribute one via META-INF/services");
    }
    if (found.size() > 1) {
      List<String> names = new ArrayList<>();
      found.forEach(support -> names.add(support.getClass().getName()));
      names.sort(null);
      throw new IllegalStateException(
          "Multiple SqlDialectTestSupport implementations on the classpath ("
              + String.join(", ", names)
              + ") — exactly one store module must be active");
    }
    return found.get(0);
  }
}
