package run.ratchet.ri.security;

import run.ratchet.spi.ClassPolicy;
import run.ratchet.store.entity.JobPayload;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.objectweb.asm.Type;

/**
 * Validates job payloads for security and safety before execution.
 *
 * <p>This is the primary security gate for the job scheduler system. Every job payload must pass
 * through this validator before execution to prevent remote code execution (RCE) attacks and ensure
 * that only authorized code paths can be invoked through the scheduler.
 *
 * <p>This validator enforces multiple security checks:
 *
 * <ul>
 *   <li><b>Class Policy:</b> Only allows classes from trusted packages as determined by the
 *       configured {@link ClassPolicy}. This prevents attackers from invoking arbitrary JDK classes
 *       like {@code Runtime.getRuntime()} or third-party library code.
 *   <li><b>Method Visibility:</b> Only allows public methods. Private, protected, and
 *       package-private methods cannot be invoked through the scheduler, even if the class is
 *       allowed.
 *   <li><b>Method Existence:</b> Validates that the method exists with the exact signature
 *       specified in the payload, preventing method confusion attacks.
 * </ul>
 *
 * <p>All validation failures result in {@link SecurityException} being thrown to prevent
 * unauthorized code execution. These failures are logged at ERROR level for security monitoring and
 * audit purposes.
 *
 * <p><b>Security Design:</b> This class follows the principle of "default deny" - if any check
 * fails or encounters an error, execution is blocked.
 *
 * @see ClassPolicy
 * @see JobPayloadInputValidator for structural validation (non-security)
 */
@ApplicationScoped
public class JobSecurityValidator {

  private static final Logger log = Logger.getLogger(JobSecurityValidator.class.getName());

  /**
   * The class policy used to validate target classes.
   *
   * <p>This is injected at construction time and cannot be modified afterward, preventing runtime
   * manipulation of security boundaries.
   */
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
      log.log(Level.SEVERE, "Cannot load class " + targetClass + " for job execution", e);
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

    log.fine("Job payload validated successfully: " + targetClass + "." + payload.method());
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
