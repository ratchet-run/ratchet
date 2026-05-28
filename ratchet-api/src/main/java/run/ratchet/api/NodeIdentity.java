package run.ratchet.api;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Value type wrapping the stable, unique identifier for a scheduler node at the cluster-coordinator
 * boundary. Coordinators consume the underlying string from {@code NodeIdentityProvider} and wrap
 * it here so wakeup notifications carry source identity for self-suppression and observability.
 *
 * <p>The wrapped {@code value} must be non-null, non-blank, and match {@link #ALLOWED_PATTERN} —
 * letters, digits, and the punctuation set {@code _ . - :}. The character set is the intersection
 * of what hostnames, UUIDs, and Kubernetes pod names produce and what JMS selector string literals
 * accept without escaping. This rules out {@code '}, {@code %}, {@code \}, and similar selector
 * metacharacters that would either fail broker selector compilation (rejecting startup) or silently
 * degrade filtering on brokers with quirky escape handling.
 *
 * <p>IPv6 literals are accepted in colon-compressed form (e.g. {@code fe80:0:0:0:0:0:0:1} or {@code
 * fe80::1}). Zone identifiers ({@code %eth0}) and bracketed forms ({@code [fe80::1]}) are rejected
 * because {@code %}, {@code [}, and {@code ]} are not in the allowed set; encode any required zone
 * information out-of-band rather than in the identity string.
 *
 * @since 0.1
 */
@Incubating
public record NodeIdentity(String value) {

  /**
   * Character classes accepted in a node identity. The dot and dash are escaped inside the
   * character class for clarity even though only the dash is strictly required.
   */
  public static final Pattern ALLOWED_PATTERN = Pattern.compile("^[A-Za-z0-9_.\\-:]+$");

  public NodeIdentity {
    Objects.requireNonNull(value, "value");
    if (value.isBlank()) {
      throw new IllegalArgumentException("blank node identity");
    }
    if (!ALLOWED_PATTERN.matcher(value).matches()) {
      throw new IllegalArgumentException(
          "node identity '" + value + "' contains characters outside " + ALLOWED_PATTERN.pattern());
    }
  }
}
