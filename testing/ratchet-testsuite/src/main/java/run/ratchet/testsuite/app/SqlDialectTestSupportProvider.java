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
    return ServiceLoader.load(SqlDialectTestSupport.class)
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "No SqlDialectTestSupport registered on the classpath — the active store module"
                        + " must contribute one via META-INF/services"));
  }
}
