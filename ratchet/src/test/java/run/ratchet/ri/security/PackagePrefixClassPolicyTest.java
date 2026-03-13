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
}
