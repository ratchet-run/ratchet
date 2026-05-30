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
