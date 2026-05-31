package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * SPI that decides which payload fields are sensitive and should be masked before a payload leaves
 * the framework — whether rendered into a log line or returned from a read API such as the {@code
 * params} on a job detail. Masking is applied only on these read and observability paths; the
 * durable store payload and anything a worker needs to execute are never altered.
 *
 * <p>The built-in policy matches a fixed set of common credential and PII field names (for example
 * {@code password}, {@code token}, {@code ssn}). Deployers that need a different field set produce
 * their own implementation; the reference implementation installs the produced bean in place of the
 * built-in default.
 */
@Incubating
public interface PayloadMaskingPolicy {

  /**
   * Reports whether a payload field with the given name holds sensitive data.
   *
   * @param fieldName the field or parameter name as it appears in the payload; never {@code null}
   * @return {@code true} if the field's value should be masked in read or log output, {@code false}
   *     otherwise
   */
  boolean isSensitiveField(String fieldName);
}
