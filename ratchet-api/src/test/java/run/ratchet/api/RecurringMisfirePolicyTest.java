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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class RecurringMisfirePolicyTest {

  @Test
  void defaultsPreserveCurrentElevenExecutionCatchUp() {
    RecurringMisfirePolicy policy = RecurringMisfirePolicy.defaults();

    assertEquals(RecurringMisfirePolicy.Action.CATCH_UP, policy.action());
    assertEquals(11, policy.maxCatchUpExecutions());
  }

  @Test
  void skipAndFireOnceDoNotCarryCatchUpLimits() {
    assertEquals(
        new RecurringMisfirePolicy(RecurringMisfirePolicy.Action.SKIP, 0),
        RecurringMisfirePolicy.skip());
    assertEquals(
        new RecurringMisfirePolicy(RecurringMisfirePolicy.Action.FIRE_ONCE, 0),
        RecurringMisfirePolicy.fireOnce());
  }

  @Test
  void catchUpRequiresPositiveExecutionLimit() {
    assertEquals(5, RecurringMisfirePolicy.catchUp(5).maxCatchUpExecutions());
    assertThrows(IllegalArgumentException.class, () -> RecurringMisfirePolicy.catchUp(0));
    assertThrows(IllegalArgumentException.class, () -> RecurringMisfirePolicy.catchUp(-1));
  }

  @Test
  void canonicalConstructorRejectsInvalidActionLimitCombinations() {
    assertThrows(NullPointerException.class, () -> new RecurringMisfirePolicy(null, 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new RecurringMisfirePolicy(RecurringMisfirePolicy.Action.SKIP, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new RecurringMisfirePolicy(RecurringMisfirePolicy.Action.FIRE_ONCE, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new RecurringMisfirePolicy(RecurringMisfirePolicy.Action.CATCH_UP, 0));
  }
}
