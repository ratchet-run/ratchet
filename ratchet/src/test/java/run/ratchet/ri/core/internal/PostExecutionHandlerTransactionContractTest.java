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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import org.junit.jupiter.api.Test;

class PostExecutionHandlerTransactionContractTest {

  @Test
  void handlerUsesRequiresNewAndRollsBackOnCheckedExceptions() {
    Transactional transactional = PostExecutionHandler.class.getAnnotation(Transactional.class);

    assertNotNull(transactional);
    assertEquals(TxType.REQUIRES_NEW, transactional.value());
    assertArrayEquals(new Class<?>[] {Exception.class}, transactional.rollbackOn());
  }
}
