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
package run.ratchet.api;

/**
 * Pre-configured circuit breaker profiles for common use cases, referenced from {@link
 * CircuitBreakerProtected} annotations.
 *
 * <ul>
 *   <li><b>DEFAULT</b> — General internal services (50% failure threshold, 100-call window)
 *   <li><b>FAST</b> — Quick failure detection (50% threshold, 20-call window, 10s wait)
 *   <li><b>CRITICAL</b> — High-availability services (75% threshold, 200-call window, 60s wait)
 *   <li><b>EXTERNAL_API</b> — Third-party integrations (60% threshold, 50-call window, 60s wait)
 *   <li><b>CLAIM_PATH</b> — Poller store-claim path (50% threshold, 20-call window, 5s wait)
 * </ul>
 */
@Incubating
public enum CircuitBreakerProfile {
  DEFAULT,
  FAST,
  CRITICAL,
  EXTERNAL_API,
  CLAIM_PATH
}
