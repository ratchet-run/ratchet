package run.ratchet.ri.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Legacy compatibility alias for {@link run.ratchet.api.DoNotRetry}.
 *
 * <p>New code should import the API annotation so applications do not need an RI dependency.
 *
 * @deprecated use {@link run.ratchet.api.DoNotRetry} instead
 */
@Deprecated(forRemoval = true)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DoNotRetry {

  /** Mirrors {@link run.ratchet.api.DoNotRetry#value()}. */
  String value() default "";
}
