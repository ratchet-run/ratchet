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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import jakarta.persistence.Query;
import org.junit.jupiter.api.Test;

class ClaimFenceTest {
  @Test
  void bindsClaimThenAttemptsForAllOptionalCombinations() {
    for (Long claim : new Long[] {null, 7L}) {
      for (Integer attempts : new Integer[] {null, 2}) {
        Query query = mock(Query.class);
        assertSame(query, ClaimFence.bind(query, claim, attempts, 4));
        if (claim != null) verify(query).setParameter(4, claim);
        if (attempts != null) verify(query).setParameter(claim == null ? 4 : 5, attempts);
        verifyNoMoreInteractions(query);
      }
    }
  }
}
