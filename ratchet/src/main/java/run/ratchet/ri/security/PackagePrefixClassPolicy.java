package run.ratchet.ri.security;

import run.ratchet.spi.ClassPolicy;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Security policy for allowed job target classes based on package prefixes.
 *
 * <p>This class enforces strict restrictions on which classes can be loaded and executed as job
 * targets. Only classes from trusted application packages are allowed. This is a critical security
 * component that prevents arbitrary code execution attacks where a malicious actor might attempt to
 * execute system commands or load untrusted classes through the job scheduler.
 *
 * <p>The policy operates on package prefixes, meaning any class whose fully qualified name starts
 * with an allowed package prefix will be permitted. This approach allows all application code while
 * blocking JDK classes, third-party libraries, and any other potentially dangerous code paths.
 *
 * <p><b>Two-layer enforcement:</b>
 *
 * <ol>
 *   <li><b>Hardcoded denylist</b> — {@link #DENIED_EXACT} and {@link #DENIED_PREFIXES} block
 *       well-known RCE gadgets (Runtime, ProcessBuilder, reflection, scripting, JDK internals)
 *       BEFORE the allowlist is consulted. This defends against misconfigured allowlists like
 *       {@code Set.of("java")} that would otherwise permit dangerous classes.
 *   <li><b>User-supplied allowlist</b> — only classes whose fully-qualified name has one of the
 *       configured prefixes as a {@code startsWith} match are permitted.
 * </ol>
 *
 * <p><b>Prefix validation</b> — constructor rejects prefixes that would trivially defeat the
 * policy: empty strings, whitespace-only, or anything shorter than 3 characters without a dot.
 *
 * <p><b>Security Note:</b> This class is intentionally restrictive. Adding new allowed packages
 * should be done with extreme caution and only after security review.
 *
 * @see JobSecurityValidator
 */
public class PackagePrefixClassPolicy implements ClassPolicy {

  private static final Logger log = Logger.getLogger(PackagePrefixClassPolicy.class.getName());

  /**
   * Fully-qualified class names that are NEVER allowed, regardless of the configured allowlist.
   * These are well-known RCE gadgets: direct shell access, serialization sinks, and reflection
   * entry points that should never be reachable from job payloads.
   */
  private static final Set<String> DENIED_EXACT =
      Set.of(
          "java.lang.Runtime",
          "java.lang.ProcessBuilder",
          "java.lang.ProcessImpl",
          "java.io.FileOutputStream",
          "java.io.FileInputStream",
          "java.io.ObjectInputStream",
          "java.io.ObjectOutputStream");

  /**
   * Package prefixes whose contents are NEVER allowed. Entries must end with a {@code .} so the
   * {@code startsWith} match lines up on package boundaries (e.g. {@code "java.lang.reflect."}
   * matches {@code java.lang.reflect.Method} but NOT {@code java.lang.reflectX}).
   */
  private static final List<String> DENIED_PREFIXES =
      List.of(
          "java.lang.reflect.",
          "java.lang.invoke.",
          "javax.script.",
          "jdk.",
          "sun.",
          "com.sun.",
          "jdk.internal.",
          "org.codehaus.groovy.runtime.",
          "org.apache.commons.collections.functors.",
          "org.springframework.context.support.");

  /**
   * The default set of allowed package prefixes for job target classes.
   *
   * <p>This is intentionally empty -- the user must configure allowed packages for their
   * application.
   */
  private static final Set<String> DEFAULT_ALLOWED_PACKAGES = Set.of();

  /**
   * The active set of allowed package prefixes.
   *
   * <p>This is an immutable copy created at construction time to prevent modification after
   * initialization, which could be a security vulnerability.
   */
  private final Set<String> allowedPackages;

  /** Creates a new PackagePrefixClassPolicy with default (empty) allowed packages. */
  public PackagePrefixClassPolicy() {
    this(DEFAULT_ALLOWED_PACKAGES);
  }

  /**
   * Creates a new PackagePrefixClassPolicy with specified allowed packages.
   *
   * @param allowedPackages the set of package prefixes to allow
   * @throws IllegalArgumentException if any prefix is null, blank, shorter than 3 characters, or
   *     otherwise trivially unsafe (e.g. {@code ""} or {@code " "})
   */
  public PackagePrefixClassPolicy(Set<String> allowedPackages) {
    validatePrefixes(allowedPackages);
    this.allowedPackages = Set.copyOf(allowedPackages);
  }

  /**
   * Validates user-supplied prefixes. Rejects the kinds of values that would either trivially
   * bypass the policy (empty string matches every class via {@code startsWith("")}) or indicate
   * likely misconfiguration ({@code "a"} or {@code "j"} as a prefix is almost certainly wrong).
   */
  private static void validatePrefixes(Set<String> prefixes) {
    if (prefixes == null) {
      throw new IllegalArgumentException("Allowed package prefixes set must not be null");
    }
    for (String prefix : prefixes) {
      if (prefix == null) {
        throw new IllegalArgumentException("Allowed package prefix must not be null");
      }
      String trimmed = prefix.trim();
      if (trimmed.isEmpty()) {
        throw new IllegalArgumentException(
            "Allowed package prefix must not be blank — empty prefixes would match every class");
      }
      if (!trimmed.equals(prefix)) {
        throw new IllegalArgumentException(
            "Allowed package prefix must not have leading or trailing whitespace: '"
                + prefix
                + "'");
      }
      if (prefix.length() < 3) {
        throw new IllegalArgumentException(
            "Allowed package prefix must be at least 3 characters: '" + prefix + "'");
      }
    }
  }

  /**
   * Gets the set of allowed package prefixes.
   *
   * @return an unmodifiable set of allowed package prefixes
   */
  public Set<String> getAllowedPackages() {
    return allowedPackages;
  }

  /**
   * Checks if a class name is allowed to be loaded and executed.
   *
   * <p>Enforcement order:
   *
   * <ol>
   *   <li>Null or empty → reject
   *   <li>Hardcoded denylist (exact) → reject
   *   <li>Hardcoded denylist (prefix) → reject
   *   <li>User allowlist → accept only on prefix match
   * </ol>
   *
   * @param className the fully qualified class name to check
   * @return true if the class is allowed, false otherwise
   */
  @Override
  public boolean isAllowed(String className) {
    if (className == null || className.isEmpty()) {
      return false;
    }

    // Layer 1: hardcoded denylist — blocks well-known RCE gadgets regardless of allowlist config.
    if (DENIED_EXACT.contains(className)) {
      log.warning("Class " + className + " is on the hardcoded denylist (exact match)");
      return false;
    }
    for (String deniedPrefix : DENIED_PREFIXES) {
      if (className.startsWith(deniedPrefix)) {
        log.warning(
            "Class " + className + " is on the hardcoded denylist (prefix: " + deniedPrefix + ")");
        return false;
      }
    }

    // Layer 2: user allowlist.
    for (String allowedPackage : allowedPackages) {
      if (className.startsWith(allowedPackage)) {
        return true;
      }
    }

    log.warning("Class " + className + " is not in allowed packages: " + allowedPackages);
    return false;
  }
}
