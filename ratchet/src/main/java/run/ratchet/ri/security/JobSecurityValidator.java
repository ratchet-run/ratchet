package run.ratchet.ri.security;

import run.ratchet.spi.ClassPolicy;
import run.ratchet.store.entity.JobPayload;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.jboss.logging.Logger;
import org.objectweb.asm.Type;

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

  /** Required by CDI proxy. */
  protected JobSecurityValidator() {
    this.classPolicy = null;
  }

  @Inject
  public JobSecurityValidator(ClassPolicy classPolicy) {
    this.classPolicy = classPolicy;
  }

  /**
   * Validates a job payload before execution.
   *
   * <p>Performs the following checks:
   *
   * <ol>
   *   <li>Class name is allowed by the configured policy
   *   <li>Class can be loaded
   *   <li>Method exists with correct signature
   *   <li>Method is public
   * </ol>
   *
   * @param payload the job payload to validate
   * @throws SecurityException if any validation check fails
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

    // Check class policy
    if (!classPolicy.isAllowed(targetClass)) {
      throw new SecurityException("Class " + targetClass + " is not allowed for job execution.");
    }

    // Load and validate class
    Class<?> clazz;
    try {
      clazz = Class.forName(targetClass, true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      log.errorf(e, "Cannot load class %s for job execution", targetClass);
      throw new SecurityException(
          "Class " + targetClass + " cannot be loaded: " + e.getMessage(), e);
    }

    // Find and validate method (public only)
    Method method = findMethod(clazz, payload);
    if (method == null) {
      // Check if the method exists but is non-public -- give a targeted error
      Method nonPublic = findDeclaredMethod(clazz, payload);
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

    // Validate method visibility - must be public (belt-and-suspenders; findMethod uses getMethods)
    int modifiers = method.getModifiers();
    if (!Modifier.isPublic(modifiers)) {
      throw new SecurityException(
          "Method "
              + payload.method()
              + " in class "
              + targetClass
              + " is not public. Only public methods can be scheduled.");
    }

    log.debugf("Job payload validated successfully: %s.%s", targetClass, payload.method());
  }

  /**
   * Finds a public method in the given class matching the payload specification.
   *
   * @param clazz the class to search for the method
   * @param payload the job payload containing method name and ASM method descriptor
   * @return the matching Method object, or null if no public method with the exact signature exists
   */
  private Method findMethod(Class<?> clazz, JobPayload payload) {
    for (Method m : clazz.getMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        return m;
      }
    }
    return null;
  }

  /**
   * Finds a method of any visibility in the given class matching the payload specification.
   *
   * @param clazz the class to search for the method
   * @param payload the job payload containing method name and ASM method descriptor
   * @return the matching Method regardless of visibility, or null if not found at all
   */
  private Method findDeclaredMethod(Class<?> clazz, JobPayload payload) {
    for (Method m : clazz.getDeclaredMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        return m;
      }
    }
    return null;
  }
}
