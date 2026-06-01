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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions.SecurityOptions;

class MaskPayloadsOptionTest {

  @Test
  void securityOptions_carriesMaskPayloadsFlag() {
    SecurityOptions enabled = new SecurityOptions(false, true, true);
    SecurityOptions disabled = new SecurityOptions(false, true, false);

    assertTrue(enabled.maskPayloads());
    assertFalse(disabled.maskPayloads());
  }

  @Test
  void securityBuilder_maskPayloadsDefaultsToFalse() {
    RatchetOptions options =
        RatchetOptions.builder().security(security -> security.redactEmails(true)).build();

    assertFalse(options.security().maskPayloads());
  }

  @Test
  void securityBuilder_maskPayloadsOptInIsHonored() {
    RatchetOptions options =
        RatchetOptions.builder().security(security -> security.maskPayloads(true)).build();

    assertTrue(options.security().maskPayloads());
  }
}
