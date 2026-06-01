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
 * Reserved execution-target names — the single source of truth for the pool names a job may be
 * routed to.
 *
 * <p>An execution target labels which configured executor pool runs a job. It is a routing label,
 * not a guarantee about thread type: the container decides whether a pool is backed by virtual or
 * platform threads. {@link #PLATFORM} always exists; {@link #VIRTUAL} exists only when a virtual
 * executor is configured, otherwise a virtual-targeted job falls back to platform.
 *
 * <p>Today only these two names are reserved. The persisted representation is an arbitrary string
 * so that future named pools require no schema migration; the reserved names defined here are what
 * the builder, router, and registry validate against.
 */
public final class ExecutorTargets {

  /** The mandatory platform executor pool. Always present. */
  public static final String PLATFORM = "platform";

  /** The optional virtual executor pool. Present only when a virtual executor is configured. */
  public static final String VIRTUAL = "virtual";

  private ExecutorTargets() {}
}
