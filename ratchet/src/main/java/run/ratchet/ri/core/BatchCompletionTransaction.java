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
package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Runs completion work (a job's terminal transition or a batch parent's completion) in its own new
 * transaction. Also used for conflict-resolution reads after a submission transaction has failed.
 */
@ApplicationScoped
public class BatchCompletionTransaction {
  /** Runs a partition sealing page in the caller transaction, or starts one. */
  @Transactional(value = Transactional.TxType.REQUIRED, rollbackOn = Exception.class)
  public <T> T required(Supplier<T> work) {
    return Objects.requireNonNull(work, "work must not be null").get();
  }

  @Transactional(value = Transactional.TxType.REQUIRES_NEW, rollbackOn = Exception.class)
  public <T> T complete(Supplier<T> work) {
    return Objects.requireNonNull(work, "work must not be null").get();
  }
}
