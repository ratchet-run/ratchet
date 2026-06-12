/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    // Regression for the broad-allowlist misconfiguration attack scenario.
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("java.lang."));
    assertFalse(
        policy.isAllowed("java.lang.Runtime"),
        "java.lang.Runtime must be blocked by hardcoded denylist even with 'java.lang.' allowed");
  }

  @Test
  void denylist_rejectsProcessBuilder() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("java.lang."));
    assertFalse(policy.isAllowed("java.lang.ProcessBuilder"));
  }

  @Test
  void denylist_rejectsSystem() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("java.lang."));
    assertFalse(policy.isAllowed("java.lang.System"));
  }

  @Test
  void denylist_rejectsObjectInputStream() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("java.io."));
    assertFalse(policy.isAllowed("java.io.ObjectInputStream"));
  }

  @Test
  void denylist_rejectsReflectionEntries() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("java.lang."));
    assertFalse(policy.isAllowed("java.lang.reflect.Method"));
    assertFalse(policy.isAllowed("java.lang.invoke.MethodHandles"));
  }

  @Test
  void denylist_rejectsScriptEngine() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("javax.script."));
    assertFalse(policy.isAllowed("javax.script.ScriptEngineManager"));
  }

  @Test
  void denylist_rejectsNamingFactories() {
    // javax.naming. is now denied as a PREFIX, not just the exact InitialContext, so LDAP/RMI
    // reference factories and the SPI package are blocked too.
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("javax.naming."));
    assertFalse(policy.isAllowed("javax.naming.spi.ObjectFactory"));
    assertFalse(policy.isAllowed("javax.naming.ldap.LdapContext"));
    assertFalse(policy.isAllowed("javax.naming.InitialContext"));
  }

  @Test
  void denylist_rejectsJdkInternals() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("jdk.internal."));
    assertFalse(policy.isAllowed("jdk.internal.misc.Unsafe"));
    PackagePrefixClassPolicy sunPolicy = new PackagePrefixClassPolicy(Set.of("sun.misc."));
    assertFalse(sunPolicy.isAllowed("sun.misc.Unsafe"));
  }

  @Test
  void denylist_rejectsDeserializationGadgets() {
    PackagePrefixClassPolicy commons = new PackagePrefixClassPolicy(Set.of("org.apache.commons."));
    assertFalse(commons.isAllowed("org.apache.commons.collections.functors.InvokerTransformer"));
    assertFalse(commons.isAllowed("org.apache.commons.collections4.functors.InvokerTransformer"));
    assertFalse(commons.isAllowed("org.apache.commons.beanutils.BeanComparator"));

    PackagePrefixClassPolicy groovy = new PackagePrefixClassPolicy(Set.of("groovy.lang."));
    assertFalse(groovy.isAllowed("groovy.lang.GroovyShell"));

    PackagePrefixClassPolicy codehaus =
        new PackagePrefixClassPolicy(Set.of("org.codehaus.groovy."));
    assertFalse(codehaus.isAllowed("org.codehaus.groovy.runtime.MethodClosure"));

    PackagePrefixClassPolicy snake = new PackagePrefixClassPolicy(Set.of("org.yaml."));
    assertFalse(snake.isAllowed("org.yaml.snakeyaml.Yaml"));

    PackagePrefixClassPolicy bsh = new PackagePrefixClassPolicy(Set.of("bsh.x."));
    assertFalse(bsh.isAllowed("bsh.Interpreter"));

    PackagePrefixClassPolicy spring = new PackagePrefixClassPolicy(Set.of("org.springframework."));
    assertFalse(
        spring.isAllowed("org.springframework.context.support.FileSystemXmlApplicationContext"));
  }

  @Test
  void constructor_rejectsSingleTopLevelSegmentPrefix() {
    // A single top-level segment like "com." or "java." matches nearly the whole classpath and
    // would defeat the allowlist, so it must be rejected at construction.
    assertThrows(
        IllegalArgumentException.class, () -> new PackagePrefixClassPolicy(Set.of("com.")));
    assertThrows(
        IllegalArgumentException.class, () -> new PackagePrefixClassPolicy(Set.of("org.")));
    assertThrows(
        IllegalArgumentException.class, () -> new PackagePrefixClassPolicy(Set.of("java.")));
    assertThrows(
        IllegalArgumentException.class, () -> new PackagePrefixClassPolicy(Set.of("javax.")));
    assertThrows(
        IllegalArgumentException.class, () -> new PackagePrefixClassPolicy(Set.of("jakarta.")));
    // Without the trailing dot too.
    assertThrows(IllegalArgumentException.class, () -> new PackagePrefixClassPolicy(Set.of("net")));
  }

  @Test
  void constructor_acceptsTwoSegmentPrefix() {
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("com.example."));
    assertTrue(policy.isAllowed("com.example.MyJob"));
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
  void constructor_acceptsShortTwoSegmentPrefix() {
    // A short but two-segment prefix is fine; only single top-level segments are rejected.
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("a.b."));
    assertTrue(policy.isAllowed("a.b.Baz"));
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
  void resultType_deniedByDefaultEvenWhenInvocationAllowed() {
    // A class allowed for invocation is NOT instantiable from a stored result type unless it was
    // also opted in to the separate, narrower result-type allowlist.
    PackagePrefixClassPolicy policy = new PackagePrefixClassPolicy(Set.of("com.example."));
    assertTrue(policy.isAllowed("com.example.MyJob"));
    assertFalse(policy.isAllowedForResultType("com.example.MyJob"));
  }

  @Test
  void resultType_allowedWhenOptedIn() {
    PackagePrefixClassPolicy policy =
        new PackagePrefixClassPolicy(Set.of("com.example."), Set.of("com.example.dto."));
    assertTrue(policy.isAllowedForResultType("com.example.dto.OrderResult"));
    // The invocation allowlist still does not grant result-type instantiation outside the dto pkg.
    assertFalse(policy.isAllowedForResultType("com.example.MyJob"));
  }

  @Test
  void resultType_respectsDenylist() {
    PackagePrefixClassPolicy policy =
        new PackagePrefixClassPolicy(Set.of("com.example."), Set.of("java.lang."));
    assertFalse(policy.isAllowedForResultType("java.lang.Runtime"));
  }

  @Test
  void allowlist_prefixNormalizationIdempotent() {
    // Explicit trailing dot and absent trailing dot must produce the same policy.
    PackagePrefixClassPolicy withDot = new PackagePrefixClassPolicy(Set.of("com.example."));
    PackagePrefixClassPolicy withoutDot = new PackagePrefixClassPolicy(Set.of("com.example"));
    assertEquals(withDot.getAllowedPackages(), withoutDot.getAllowedPackages());
  }
}
