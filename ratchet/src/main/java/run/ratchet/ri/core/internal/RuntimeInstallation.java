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
package run.ratchet.ri.core.internal;

/**
 * Installs and removes one process-wide runtime seam for a specific Ratchet runtime owner.
 *
 * <p>Implementations must reject a conflicting owner from {@link #install(Object)} without
 * replacing the current value. {@link #uninstall(Object)} must be idempotent and must not clear a
 * value owned by another runtime.
 */
public interface RuntimeInstallation {

  /** Installs this seam for {@code ownerToken}. */
  void install(Object ownerToken);

  /** Removes this seam only when it is currently owned by {@code ownerToken}. */
  void uninstall(Object ownerToken);
}
