package run.ratchet.ri.security;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.jboss.logging.Logger;
import run.ratchet.spi.ClassPolicy;

/**
 * Allowlist-based {@link ClassPolicy} that permits job target classes only if their fully-qualified
 * name starts with a configured package prefix. A hardcoded denylist of RCE gadgets is checked
 * first, regardless of the allowlist. Constructor rejects prefixes shorter than 3 characters or
 * containing leading/trailing whitespace.
 *
 * <p>Configured allowlist prefixes are normalized to end with {@code .} so matches line up on
 * package boundaries: configuring {@code "com.foo"} matches {@code com.foo.Bar} but NOT {@code
 * com.foobar.Gadget}. This mirrors the {@link #DENIED_PREFIXES} invariant.
 *
 * @see JobSecurityValidator
 */
public class PackagePrefixClassPolicy implements ClassPolicy {

  private static final Logger log = Logger.getLogger(PackagePrefixClassPolicy.class);

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

  private final Set<String> allowedPackages;

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
    this.allowedPackages = normalize(allowedPackages);
  }

  private static Set<String> normalize(Set<String> prefixes) {
    Set<String> normalized = new LinkedHashSet<>(prefixes.size());
    for (String prefix : prefixes) {
      normalized.add(prefix.endsWith(".") ? prefix : prefix + ".");
    }
    return Set.copyOf(normalized);
  }

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

  public Set<String> getAllowedPackages() {
    return allowedPackages;
  }

  /**
   * Returns true only if {@code className} passes the hardcoded denylist and matches at least one
   * configured allowlist prefix.
   */
  @Override
  public boolean isAllowed(String className) {
    if (className == null || className.isEmpty()) {
      return false;
    }

    if (DENIED_EXACT.contains(className)) {
      log.warnf("Class %s is on the hardcoded denylist (exact match)", className);
      return false;
    }
    for (String deniedPrefix : DENIED_PREFIXES) {
      if (className.startsWith(deniedPrefix)) {
        log.warnf("Class %s is on the hardcoded denylist (prefix: %s)", className, deniedPrefix);
        return false;
      }
    }

    for (String allowedPackage : allowedPackages) {
      if (className.startsWith(allowedPackage)) {
        return true;
      }
    }

    log.warnf("Class %s is not in allowed packages: %s", className, allowedPackages);
    return false;
  }
}
