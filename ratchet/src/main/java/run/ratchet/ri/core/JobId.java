package run.ratchet.ri.core;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** CDI qualifier that associates a bean with a specific job execution context; used to wire a per-job logger instance. */
@Retention(RetentionPolicy.RUNTIME)
public @interface JobId {

  long value();
}
