package run.ratchet.spi;

import java.net.SocketTimeoutException;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import run.ratchet.api.exception.RatchetTransientStoreException;

/** Bounded exception families for metrics tags. */
public enum ExceptionFamily {
  TRANSIENT,
  TIMEOUT,
  VALIDATION,
  BUSINESS,
  UNKNOWN;

  public static ExceptionFamily classify(Throwable throwable) {
    if (throwable == null) {
      return UNKNOWN;
    }
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    boolean transientSeen = false;
    boolean validationSeen = false;
    boolean businessSeen = false;
    Throwable current = throwable;
    while (current != null && seen.add(current)) {
      if (isTimeout(current)) {
        return TIMEOUT;
      }
      transientSeen |= isTransient(current);
      validationSeen |= isValidation(current);
      businessSeen |= isBusiness(current);
      current = current.getCause();
    }
    if (transientSeen) {
      return TRANSIENT;
    }
    if (validationSeen) {
      return VALIDATION;
    }
    if (businessSeen) {
      return BUSINESS;
    }
    return UNKNOWN;
  }

  private static boolean isTimeout(Throwable throwable) {
    return throwable instanceof TimeoutException
        || throwable instanceof SocketTimeoutException
        || throwable instanceof InterruptedByTimeoutException
        || isNamed(throwable, "java.net.http.HttpTimeoutException");
  }

  private static boolean isTransient(Throwable throwable) {
    return throwable instanceof RatchetTransientStoreException
        || isNamed(throwable, "java.sql.SQLTransientException")
        || isNamed(throwable, "java.sql.SQLRecoverableException");
  }

  private static boolean isValidation(Throwable throwable) {
    String className = throwable.getClass().getName();
    return throwable instanceof IllegalArgumentException
        || "jakarta.validation.ValidationException".equals(className)
        || "jakarta.validation.ConstraintViolationException".equals(className);
  }

  private static boolean isBusiness(Throwable throwable) {
    if (!(throwable instanceof Exception)) {
      return false;
    }
    if (!(throwable instanceof RuntimeException)) {
      return true;
    }
    Package errorPackage = throwable.getClass().getPackage();
    String packageName = errorPackage != null ? errorPackage.getName() : "";
    return !(packageName.startsWith("java.")
        || packageName.startsWith("javax.")
        || packageName.startsWith("jakarta.")
        || packageName.startsWith("sun.")
        || packageName.startsWith("com.sun.")
        || packageName.startsWith("org.jboss."));
  }

  private static boolean isNamed(Throwable throwable, String className) {
    Class<?> current = throwable.getClass();
    while (current != null) {
      if (className.equals(current.getName())) {
        return true;
      }
      current = current.getSuperclass();
    }
    return false;
  }
}
