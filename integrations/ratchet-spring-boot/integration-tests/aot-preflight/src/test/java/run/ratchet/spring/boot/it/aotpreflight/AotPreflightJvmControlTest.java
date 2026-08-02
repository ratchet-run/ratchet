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
package run.ratchet.spring.boot.it.aotpreflight;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    classes = AotPreflightApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
class AotPreflightJvmControlTest {

  @Autowired private AotPreflightScenarios scenarios;

  @Test
  void methodReferencesResolveWithoutConsumerHints() {
    assertPassed(scenarios.runMethodReferences());
  }

  @Test
  void inlineWrapperResolvesFromAnnotatedNonBeanCaller() {
    assertPassed(scenarios.runWrapperLambda());
  }

  @Test
  void nestedPayloadRoundTripsThroughJsonb() {
    assertPassed(scenarios.runJsonbPayloadRoundTrip());
  }

  @Test
  void jobPayloadInvokerMaterializesAndInvokes() {
    assertPassed(scenarios.runJobPayloadInvocation());
  }

  @Test
  void manifestRejectsReachableButUnregisteredTarget() {
    assertPassed(scenarios.runManifestRejection());
  }

  private static void assertPassed(AotPreflightScenarios.Evidence evidence) {
    assertTrue(evidence.passed(), evidence.scenario() + ": " + evidence.detail());
  }
}
