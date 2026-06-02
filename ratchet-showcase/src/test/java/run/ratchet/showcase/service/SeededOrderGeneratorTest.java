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
package run.ratchet.showcase.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import run.ratchet.showcase.domain.GeneratedOrder;

class SeededOrderGeneratorTest {

  private final SeededOrderGenerator generator = new SeededOrderGenerator();

  @Test
  void sameSeedAndSequenceProduceSameOrder() {
    GeneratedOrder first = generator.generate(1234L, 42L, 0.35);
    GeneratedOrder second = generator.generate(1234L, 42L, 0.35);

    assertEquals(first, second);
  }

  @Test
  void differentSeedChangesGeneratedOrder() {
    GeneratedOrder first = generator.generate(1234L, 42L, 0.35);
    GeneratedOrder second = generator.generate(9876L, 42L, 0.35);

    assertNotEquals(first, second);
  }

  @Test
  void failureMixIncreasesBadPaymentPressureDeterministically() {
    int lowFailures = countBadPayments(0.05);
    int highFailures = countBadPayments(0.85);

    assertTrue(highFailures > lowFailures);
  }

  @Test
  void failureMixIncreasesReviewRiskDeterministically() {
    int lowRisk = countReviewRisk(0.05);
    int highRisk = countReviewRisk(1.0);

    assertTrue(highRisk > lowRisk);
  }

  private int countBadPayments(double failureMix) {
    int failures = 0;
    for (int i = 1; i <= 250; i++) {
      String profile = generator.generate(44L, i, failureMix).paymentProfile();
      if (!"NORMAL".equals(profile)) {
        failures++;
      }
    }
    return failures;
  }

  private int countReviewRisk(double failureMix) {
    int reviews = 0;
    for (int i = 1; i <= 250; i++) {
      if (generator.generate(44L, i, failureMix).fraudScore() >= 70) {
        reviews++;
      }
    }
    return reviews;
  }
}
