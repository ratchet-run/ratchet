package run.ratchet.store.util;

import java.util.Locale;
import java.util.Set;
import run.ratchet.spi.PayloadMaskingPolicy;

/**
 * Built-in {@link PayloadMaskingPolicy} that matches a fixed set of common credential and PII field
 * names. Most markers match as a case-insensitive substring (so {@code apiKey} and {@code
 * config.apiKeyHeader} both match). Short markers that are common English substrings ({@code pin},
 * {@code cvv}, {@code ssn}) match only on word boundaries to avoid false positives like {@code
 * spinner} or {@code session}.
 *
 * <p>This is the policy {@link PayloadMaskingPolicyHolder} returns when no policy has been
 * installed, so the default masking behavior is identical whether or not a CDI container is
 * present.
 */
public final class DefaultPayloadMaskingPolicy implements PayloadMaskingPolicy {

  private static final Set<String> SENSITIVE_FIELDS =
      Set.of(
          "password",
          "passwd",
          "pwd",
          "secret",
          "token",
          "apikey",
          "api_key",
          "apisecret",
          "api_secret",
          "accesskey",
          "access_key",
          "accesstoken",
          "access_token",
          "refreshtoken",
          "refresh_token",
          "auth",
          "authorization",
          "credential",
          "credentials",
          "privatekey",
          "private_key",
          "ssn",
          "socialsecuritynumber",
          "social_security_number",
          "creditcard",
          "credit_card",
          "cardnumber",
          "card_number",
          "cvv",
          "pin");

  // Short markers that are common English substrings (e.g. "pin" inside "spinner")
  // and so must match only on word boundaries; everything else uses plain substring.
  private static final Set<String> SHORT_BOUNDARY_MARKERS = Set.of("pin", "cvv", "ssn");

  @Override
  public boolean isSensitiveField(String fieldName) {
    if (fieldName == null) {
      return false;
    }
    String lower = fieldName.toLowerCase(Locale.ROOT);
    for (String marker : SENSITIVE_FIELDS) {
      if (SHORT_BOUNDARY_MARKERS.contains(marker)) {
        if (containsAsWord(lower, marker)) {
          return true;
        }
      } else if (lower.contains(marker)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsAsWord(String haystack, String needle) {
    int idx = haystack.indexOf(needle);
    while (idx >= 0) {
      boolean leftOk = idx == 0 || !Character.isLetterOrDigit(haystack.charAt(idx - 1));
      int after = idx + needle.length();
      boolean rightOk =
          after == haystack.length() || !Character.isLetterOrDigit(haystack.charAt(after));
      if (leftOk && rightOk) {
        return true;
      }
      idx = haystack.indexOf(needle, idx + 1);
    }
    return false;
  }
}
