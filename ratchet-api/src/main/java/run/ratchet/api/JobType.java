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

/** Public job categories exposed in events and SPIs. */
public enum JobType {
  /** A one-shot background task executed once and not rescheduled. */
  SINGLE,

  /** Automatically rescheduled job based on cron expression or fixed interval. */
  RECURRING,

  /** A batch-processing job comprising multiple child items processed in parallel. */
  BATCH,

  /** A sequenced chain of tasks executed in order, one after the other. */
  CHAIN,

  /** Workflow-driven execution using conditional branches or join semantics. */
  WORKFLOW,

  /** Scheduler-managed system work, not user-creatable. */
  SYSTEM
}
