package run.ratchet.ri.security;

import run.ratchet.spi.ClassPolicy;
import run.ratchet.store.entity.JobPayload;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.jboss.logging.Logger;

/**
 * Primary security gate for job execution. Enforces class-policy allowlisting, method visibility
 * (public only), and signature existence checks. Default-deny: any failure blocks execution.
 *
 * @see ClassPolicy
 * @see JobPayloadInputValidator for structural validation (non-security)
 */
@ApplicationScoped
public class JobSecurityValidator {

  private static final Logger log = Logger.getLogger(JobSecurityValidator.class);

  private final ClassPolicy classPolicy;

  // CDI proxy
  protected JobSecurityValidator() {
    this.classPolicy = null;
  }

  @Inject
  public JobSecurityValidator(ClassPolicy classPolicy) {
    this.classPolicy = classPolicy;
  }

  /**
   * @throws SecurityException if the class or method fails policy or visibility checks
   * @throws NoSuchMethodException if the method does not exist
   */
  public void validate(JobPayload payload) throws NoSuchMethodException {
    if (payload == null) {
      throw new SecurityException("Job payload cannot be null");
    }

    String targetClass = payload.target();
    if (targetClass == null || targetClass.isEmpty()) {
      throw new SecurityException("Job payload target class cannot be null or empty");
    }

    if (!classPolicy.isAllowed(targetClass)) {
      throw new SecurityException("Class " + targetClass + " is not allowed for job execution.");
    }

    Class<?> clazz;
    try {
      clazz = Class.forName(targetClass, true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      log.errorf(e, "Cannot load class %s for job execution", targetClass);
      throw new SecurityException(
          "Class " + targetClass + " cannot be loaded: " + e.getMessage(), e);
    }

    Method method = MethodLookup.findMethod(clazz, payload);
    if (method == null) {
      Method nonPublic = MethodLookup.findDeclaredMethod(clazz, payload);
      if (nonPublic != null) {
        String visibility =
            Modifier.isPrivate(nonPublic.getModifiers())
                ? "private"
                : Modifier.isProtected(nonPublic.getModifiers()) ? "protected" : "package-private";
        throw new SecurityException(
            "Method "
                + payload.method()
                + " in class "
                + targetClass
                + " is "
                + visibility
                + " -- only public methods can be scheduled as jobs. "
                + "Change the method visibility to public.");
      }
      throw new NoSuchMethodException(
          "Method "
              + payload.method()
              + " with descriptor "
              + payload.methodDescriptor()
              + " not found in class "
              + targetClass);
    }

    int modifiers = method.getModifiers();
    if (!Modifier.isPublic(modifiers)) {
      throw new SecurityException(
          "Method "
              + payload.method()
              + " in class "
              + targetClass
              + " is not public. Only public methods can be scheduled.");
    }

    log.debugf("Validated: %s.%s", targetClass, payload.method());
  }
}
