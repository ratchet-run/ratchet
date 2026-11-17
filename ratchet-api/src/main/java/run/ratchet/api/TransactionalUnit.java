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
 * One unit of work for {@link JobContext#runInTransaction(TransactionalUnit)}. Ratchet runs it in a
 * transaction on its own datasource and saves the returned checkpoint in the same transaction.
 *
 * @since 0.6
 */
@Incubating
@FunctionalInterface
public interface TransactionalUnit {
  /**
   * Does one unit of work, starting after {@code checkpoint}, and returns the next checkpoint.
   *
   * @param checkpoint the latest committed checkpoint, or {@code null} when none is saved
   * @return the next checkpoint; never {@code null}. Return your own end marker after the last
   *     unit.
   * @throws Exception any failure; the unit's work and checkpoint roll back
   */
  String run(@Nullable String checkpoint) throws Exception;
}
