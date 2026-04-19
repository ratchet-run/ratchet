package run.ratchet.spi;

import run.ratchet.api.exception.RatchetTransientStoreException;
import java.util.concurrent.TimeoutException;

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
    if (hasCause(throwable, ExceptionFamily::isTimeout)) {
      return TIMEOUT;
    }
    if (hasCause(throwable, ExceptionFamily::isTransient)) {
      return TRANSIENT;
    }
    if (hasCause(throwable, ExceptionFamily::isValidation)) {
      return VALIDATION;
    }
    if (hasCause(throwable, ExceptionFamily::isBusiness)) {
      return BUSINESS;
    }
    return UNKNOWN;
  }

  private static boolean hasCause(Throwable throwable, java.util.function.Predicate<Throwable> test) {
    Throwable current = throwable;
    while (current != null) {
      if (test.test(current)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static boolean isTimeout(Throwable throwable) {
    return throwable instanceof TimeoutException
        || throwable instanceof java.net.SocketTimeoutException
        || throwable instanceof java.nio.channels.InterruptedByTimeoutException
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
