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

import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Store-specific behavior used by the shared Spring TCK fixtures. */
public interface StoreTckBinding {

  /** Starts any container required by this store before a TCK class runs. */
  default void beforeAll(ExtensionContext context) throws Exception {}

  /** Returns the complete property set for the main store-backed TCK context. */
  String[] mainContextProperties();

  /** Returns the complete property set for the clocked in-memory TCK context. */
  String[] clockedContextProperties();

  /** Returns the package containing this store module's local fixture jobs. */
  String applicationPackage();

  /** Returns all packages allowed to supply persisted TCK job targets. */
  default Set<String> allowedPackages() {
    return Set.of(
        applicationPackage(),
        TckConfiguration.APPLICATION_PACKAGE,
        TckConfiguration.TCK_PACKAGE);
  }

  /** Returns the stable property value used by Spring's class-policy binding. */
  default String allowedPackagesProperty() {
    return String.join(
        ",",
        applicationPackage(),
        TckConfiguration.APPLICATION_PACKAGE,
        TckConfiguration.TCK_PACKAGE);
  }

  /** Returns the SQL migration dialect, or empty for a non-SQL store. */
  default Optional<String> migrationDialect() {
    return Optional.empty();
  }

  /** Deletes all persisted scheduler state for the current TCK context. */
  void clearStore();

  /** Whether this store supports the caller-transaction rollback contracts. */
  default boolean supportsCallerTransactionRollback() {
    return false;
  }

  /** Diagnostic name retained in main-runtime cleanup failures. */
  default String runtimeName() {
    return "SpringRatchetTckRuntime";
  }

  /** Diagnostic name retained in clocked-runtime cleanup failures. */
  default String clockedRuntimeName() {
    return "SpringClockedTckRuntime";
  }

  /** Maximum number of whole-transaction store-cleanup attempts. */
  default int storeClearAttempts() {
    return 1;
  }

  /** Whether a failed store cleanup may be retried from outside its transaction. */
  default boolean isRetryableStoreClearFailure(Throwable failure) {
    return false;
  }

  /** Backoff between whole-transaction store-cleanup attempts. */
  default long storeClearRetryBackoffMillis() {
    return 0L;
  }
}
