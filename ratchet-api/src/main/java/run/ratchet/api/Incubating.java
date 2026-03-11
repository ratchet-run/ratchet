package run.ratchet.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an API element as incubating — it may change in incompatible ways in future releases
 * without following the normal deprecation cycle.
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PACKAGE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Incubating {}
