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
package run.ratchet.coordinator.hazelcast;

import com.hazelcast.core.HazelcastInstance;

/**
 * SPI for resolving the {@link HazelcastInstance} the coordinator publishes / subscribes on. Payara
 * deployments use the JNDI-driven default {@code payara/Hazelcast}; standalone deployments supply a
 * higher-priority {@code @Alternative} bean that wraps a programmatically-constructed instance.
 *
 * @apiNote Implementations MUST be CDI beans (typically {@code @ApplicationScoped} or
 *     {@code @Dependent}). The coordinator resolves the active provider via {@code Instance<T>} and
 *     selects the highest-{@code @Priority} {@code @Alternative}. The coordinator does NOT own the
 *     returned {@link HazelcastInstance}: the provider is responsible for the instance's lifecycle,
 *     and the coordinator MUST NOT call {@code shutdown()} on it. The {@link #hazelcastInstance()}
 *     method MUST return a non-null instance; returning {@code null} fails the coordinator's
 *     lifecycle hook.
 */
public interface HazelcastInstanceProvider {

  /** The Hazelcast instance the coordinator looks up its topic from. */
  HazelcastInstance hazelcastInstance();
}
