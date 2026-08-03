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

/**
 * Dependency aggregator for Ratchet on Spring Boot with the MongoDB store.
 *
 * <p>This module carries no code of its own. Adding {@code ratchet-spring-boot-starter-mongodb} to
 * an application pulls in:
 *
 * <ul>
 *   <li>{@code ratchet-spring-boot-autoconfigure}, Ratchet's core Spring Boot auto-configuration.
 *   <li>{@code ratchet-spring-boot-autoconfigure-mongodb}, the MongoDB-specific auto-configuration.
 *   <li>{@code ratchet-store-mongodb}, Ratchet's MongoDB store implementation.
 *   <li>{@code mongodb-driver-sync}, the MongoDB Java driver.
 *   <li>Yasson, the JSON-B provider Ratchet uses to serialize job payloads.
 * </ul>
 *
 * <p>The MongoDB flavor is isolated from the JPA flavor: it does not require and should not be
 * combined with {@code ratchet-spring-boot-starter}. An application should not add separate copies
 * of {@code ratchet-store-mongodb} or {@code mongodb-driver-sync}; the starter already bundles
 * them.
 *
 * @see <a href="https://ratchet.run/docs/deployment/spring-boot">Spring Boot deployment guide</a>
 */
package run.ratchet.spring.boot.starter.mongodb;
