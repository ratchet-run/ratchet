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
import jakarta.transaction.Transactional.TxType;
import java.util.concurrent.Callable;

/** Transaction boundary shared by a job body and its success completion. */
@ApplicationScoped
public class JobTransaction {
  private static final ThreadLocal<Integer> DEPTH = new ThreadLocal<>();

  @Transactional(value = TxType.REQUIRED, rollbackOn = Exception.class)
  public <T> T run(Callable<T> work) throws Exception {
    return insideCarrier(work);
  }

  /** Internal carrier marker, shared by all integration variants. */
  public static boolean active() {
    return DEPTH.get() != null;
  }

  protected static <T> T insideCarrier(Callable<T> work) throws Exception {
    Integer previous = DEPTH.get();
    DEPTH.set(previous == null ? 1 : previous + 1);
    try {
      return work.call();
    } finally {
      if (previous == null) DEPTH.remove();
      else DEPTH.set(previous);
    }
  }
}
