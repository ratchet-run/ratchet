package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import org.junit.jupiter.api.Test;

class PackagePrefixClassPolicyTest {

  @Test
  void emptyPrefixesRejectsAll() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of());
    assertFalse(policy.isAllowed("com.example.MyJob"));
  }

  @Test
  void defaultConstructorRejectsAll() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy();
    assertFalse(policy.isAllowed("com.example.MyJob"));
  }

  @Test
  void exactPrefixAllows() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("com.example."));
    assertTrue(policy.isAllowed("com.example.MyJob"));
  }

  @Test
  void prefixMismatchRejects() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("com.example."));
    assertFalse(policy.isAllowed("com.other.MyJob"));
  }

  @Test
  void nullClassNameReturnsFalse() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("com.example."));
    assertFalse(policy.isAllowed(null));
  }

  @Test
  void emptyClassNameReturnsFalse() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("com.example."));
    assertFalse(policy.isAllowed(""));
  }

  @Test
  void multiplePrefixesAnyMatch() {
    PackagePrefixClassPolicy policy =
        new PackagePrefixClassPolicy(Set.of("com.alpha.", "com.beta.", "com.gamma."));
    assertTrue(policy.isAllowed("com.beta.SomeClass"));
    assertFalse(policy.isAllowed("com.delta.SomeClass"));
  }

  @Test
  void getAllowedPackagesReturnsDefensiveCopy() {
    Set<String> original = Set.of("com.example.");
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(original);
    Set<String> returned = policy.getAllowedPackages();
    assertThrows(UnsupportedOperationException.class, () -> returned.add("com.evil."));
    assertEquals(Set.of("com.example."), policy.getAllowedPackages());
  }

  @Test
  void denylist_rejectsRuntimeEvenWithBroadJavaAllowlist() {
    // Regression for the "Set.of('java')" misconfiguration attack scenario.
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("java."));
    assertFalse(
        policy.isAllowed("java.lang.Runtime"),
        "java.lang.Runtime must be blocked by hardcoded denylist even with 'java.' in allowlist");
  }

  @Test
  void denylist_rejectsProcessBuilder() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("java."));
    assertFalse(policy.isAllowed("java.lang.ProcessBuilder"));
  }

  @Test
  void denylist_rejectsObjectInputStream() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("java."));
    assertFalse(policy.isAllowed("java.io.ObjectInputStream"));
  }

  @Test
  void denylist_rejectsReflectionEntries() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("java."));
    assertFalse(policy.isAllowed("java.lang.reflect.Method"));
    assertFalse(policy.isAllowed("java.lang.invoke.MethodHandles"));
  }

  @Test
  void denylist_rejectsScriptEngine() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("javax."));
    assertFalse(policy.isAllowed("javax.script.ScriptEngineManager"));
  }

  @Test
  void denylist_rejectsJdkInternals() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("jdk."));
    assertFalse(policy.isAllowed("jdk.internal.misc.Unsafe"));
    assertFalse(policy.isAllowed("sun.misc.Unsafe"));
  }

  @Test
  void denylist_rejectsDeserializationGadgets() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("org."));
    assertFalse(policy.isAllowed("org.apache.commons.collections.functors.InvokerTransformer"));
    assertFalse(policy.isAllowed("org.codehaus.groovy.runtime.MethodClosure"));
    assertFalse(
        policy.isAllowed("org.springframework.context.support.FileSystemXmlApplicationContext"));
  }

  @Test
  void constructor_rejectsEmptyStringPrefix() {
    // Empty string would match every class via startsWith("").
    assertThrows(IllegalArgumentException.class, () -> new PackagePrefixClassPolicy(Set.of("")));
  }

  @Test
  void constructor_rejectsShortPrefix() {
    // 2-char prefixes are almost certainly misconfiguration (e.g. "j" or "co").
    assertThrows(IllegalArgumentException.class, () -> new PackagePrefixClassPolicy(Set.of("ab")));
  }

  @Test
  void constructor_acceptsThreeCharMinimumPrefix() {
    // 3 chars is the minimum, though this should still feel wrong in practice.
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("foo"));
    assertTrue(policy.isAllowed("foo.bar.Baz"));
  }

  @Test
  void constructor_rejectsWhitespacePrefix() {
    assertThrows(IllegalArgumentException.class, () -> new PackagePrefixClassPolicy(Set.of("   ")));
  }

  @Test
  void constructor_rejectsLeadingTrailingWhitespace() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new PackagePrefixClassPolicy(Set.of(" com.example.")));
  }

  @Test
  void allowlist_prefixWithoutTrailingDot_doesNotMatchAdjacentPackage() {
    // Regression: configuring "com.foo" without a trailing dot must not match
    // "com.foobar.Gadget" via raw startsWith. Normalization appends the dot.
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("com.foo"));
    assertTrue(policy.isAllowed("com.foo.Bar"));
    assertFalse(
        policy.isAllowed("com.foobar.Gadget"),
        "com.foobar.Gadget must not match the 'com.foo' allowlist prefix");
  }

  @Test
  void allowlist_prefixNormalizationIdempotent() {
    // Explicit trailing dot and absent trailing dot must produce the same policy.
    PackagePrefixClassPolicy withDot = new PackagePrefixClassPolicy(Set.of("com.example."));
    PackagePrefixClassPolicy withoutDot = new PackagePrefixClassPolicy(Set.of("com.example"));
    assertEquals(withDot.getAllowedPackages(), withoutDot.getAllowedPackages());
  }
}
