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
 * Dependency aggregator for Ratchet on Spring Boot with a JPA-backed SQL store.
 *
 * <p>This module carries no code of its own. Adding {@code ratchet-spring-boot-starter} to an
 * application pulls in:
 *
 * <ul>
 *   <li>{@code spring-boot-starter-data-jpa}, so the application's {@code EntityManagerFactory} and
 *       {@code JpaTransactionManager} are available for Ratchet's entities to join.
 *   <li>{@code ratchet-spring-boot-autoconfigure}, Ratchet's core Spring Boot auto-configuration.
 *   <li>{@code ratchet-spring-boot-autoconfigure-jpa}, the JPA-specific auto-configuration that
 *       wires Ratchet's stores into the shared persistence unit.
 *   <li>Yasson, the JSON-B provider Ratchet uses to serialize job payloads.
 * </ul>
 *
 * <p>The starter deliberately ships no store implementation. The application must add exactly one
 * {@code ratchet-store-*} dependency (for example {@code ratchet-store-postgresql}); adding more
 * than one causes startup to fail before the scheduler starts.
 *
 * @see <a href="https://ratchet.run/docs/deployment/spring-boot">Spring Boot deployment guide</a>
 */
package run.ratchet.spring.boot.starter;
