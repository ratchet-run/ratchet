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
package run.ratchet.api.exception;

import java.io.Serial;
import run.ratchet.api.DoNotRetry;

/** Thrown when a job names a resource with no configured limit. */
@DoNotRetry
public class ResourceNotConfiguredException extends IllegalArgumentException {

  @Serial private static final long serialVersionUID = 1L;

  private final String resourceName;

  public ResourceNotConfiguredException(String resourceName) {
    super(
        "No limit is set for resource '"
            + resourceName
            + "'. Call setResourceLimit(ResourceLimit.of(\""
            + resourceName
            + "\", maxConcurrent)) first, then re-run any job that failed for this reason with"
            + " retryJob.");
    this.resourceName = resourceName;
  }

  /** Returns the resource name with no configured limit. */
  public String resourceName() {
    return resourceName;
  }
}
