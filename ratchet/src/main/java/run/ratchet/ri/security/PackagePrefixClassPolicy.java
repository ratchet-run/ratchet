package run.ratchet.ri.security;

import run.ratchet.spi.ClassPolicy;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
 * <p>Configuration:
 *
 * <ul>
 *   <li>Default allowed packages: empty set (must be configured by the user)
 *   <li>Can be extended via constructor injection for testing or special deployments
 * </ul>
 *
 * <p><b>Security Note:</b> This class is intentionally restrictive. Adding new allowed packages
 * should be done with extreme caution and only after security review.
 *
 * @see JobSecurityValidator
 */
public class PackagePrefixClassPolicy implements ClassPolicy {

  private static final Logger log = Logger.getLogger(PackagePrefixClassPolicy.class.getName());

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

  /**
   * Compiled regex patterns for each allowed package.
   *
   * <p>These patterns are pre-compiled at construction time for performance when checking class
   * names.
   */
  private final Set<Pattern> allowedPatterns;

  /** Creates a new PackagePrefixClassPolicy with default (empty) allowed packages. */
  public PackagePrefixClassPolicy() {
    this(DEFAULT_ALLOWED_PACKAGES);
  }

  /**
   * Creates a new PackagePrefixClassPolicy with specified allowed packages.
   *
   * @param allowedPackages the set of package prefixes to allow
   */
  public PackagePrefixClassPolicy(Set<String> allowedPackages) {
    this.allowedPackages = Set.copyOf(allowedPackages);
    this.allowedPatterns =
        allowedPackages.stream()
            .map(pkg -> Pattern.compile("^" + Pattern.quote(pkg) + ".*"))
            .collect(Collectors.toSet());
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
   * @param className the fully qualified class name to check
   * @return true if the class is allowed, false otherwise
   */
  @Override
  public boolean isAllowed(String className) {
    if (className == null || className.isEmpty()) {
      return false;
    }

    // Check against package prefixes
    for (String allowedPackage : allowedPackages) {
      if (className.startsWith(allowedPackage)) {
        return true;
      }
    }

    // Check against patterns (for future extensibility)
    for (Pattern pattern : allowedPatterns) {
      if (pattern.matcher(className).matches()) {
        return true;
      }
    }

    log.warning("Class " + className + " is not in allowed packages: " + allowedPackages);
    return false;
  }
}
