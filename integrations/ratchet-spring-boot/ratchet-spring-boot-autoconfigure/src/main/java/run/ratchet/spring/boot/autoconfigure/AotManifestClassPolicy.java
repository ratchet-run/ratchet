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
package run.ratchet.spring.boot.autoconfigure;

import java.util.Objects;
import java.util.Set;
import run.ratchet.spi.ClassPolicy;

/** Intersects the invocation policy with classes registered during AOT processing. */
final class AotManifestClassPolicy implements ClassPolicy {

  private final ClassPolicy delegate;
  private final RatchetAotManifest manifest;
  private final Set<String> allowedPackages;

  AotManifestClassPolicy(
      ClassPolicy delegate, RatchetAotManifest manifest, Set<String> allowedPackages) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    this.manifest = Objects.requireNonNull(manifest, "manifest must not be null");
    this.allowedPackages = Set.copyOf(allowedPackages);
  }

  @Override
  public boolean isAllowed(String className) {
    if (!manifest.contains(className)) {
      throw new SecurityException(
          "Class '"
              + className
              + "' is not registered in "
              + RatchetAotManifest.RESOURCE_PATH
              + ". Add its package to ratchet.class-policy.allowed-packages (currently "
              + allowedPackages
              + ") and rebuild the application so Ratchet AOT processing can register it for"
              + " native execution.");
    }
    return delegate.isAllowed(className);
  }

  @Override
  public boolean isAllowedForResultType(String className) {
    return delegate.isAllowedForResultType(className);
  }
}
