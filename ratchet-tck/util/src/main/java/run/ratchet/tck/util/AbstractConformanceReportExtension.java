package run.ratchet.tck.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

/**
 * Base {@link TestExecutionListener} that produces a Markdown conformance report. Subclasses
 * declare the tier name, report path, runtime identity property, and contract groups — this class
 * handles chain-walking, result accumulation, missing-contract detection, and rendering.
 *
 * <p>The listener is scope-guarded: it writes the report only when at least one recognized contract
 * class is observed, preventing spurious empty reports from non-TCK test runs that happen to have a
 * TCK module on their classpath.
 *
 * <p>Register concrete subclasses via {@code META-INF/services} (classpath) and a {@code provides}
 * declaration in {@code module-info.java} (JPMS). Works under both Surefire and Failsafe.
 */
public abstract class AbstractConformanceReportExtension implements TestExecutionListener {

  private final Map<String, ContractResult> results = new LinkedHashMap<>();
  private Map<String, String> contractIndex; // simpleClassName → group label

  private static String resolveClassName(TestIdentifier id) {
    return id.getSource()
        .map(
            source -> {
              if (source instanceof MethodSource ms) return ms.getClassName();
              if (source instanceof ClassSource cs) return cs.getClassName();
              return null;
            })
        .orElse(null);
  }

  @Override
  public void testPlanExecutionStarted(TestPlan testPlan) {
    contractIndex = new HashMap<>();
    for (ContractGroup group : contractGroups()) {
      for (String name : group.contracts()) {
        contractIndex.put(name, group.label());
      }
    }
  }

  @Override
  public void executionFinished(TestIdentifier id, TestExecutionResult result) {
    if (!id.isTest()) return;
    String className = resolveClassName(id);
    if (className == null) return;
    String contractName = findContractName(className);
    if (contractName == null) return;
    results.computeIfAbsent(contractName, k -> new ContractResult()).record(result.getStatus());
  }

  @Override
  public void testPlanExecutionFinished(TestPlan testPlan) {
    if (results.isEmpty()) return;
    try {
      Files.createDirectories(reportPath().getParent());
      try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(reportPath()))) {
        writeReport(pw);
      }
    } catch (IOException e) {
      System.err.println("[ratchet-tck] Failed to write conformance report: " + e.getMessage());
    }
  }

  String findContractName(String className) {
    Map<String, String> index =
        Objects.requireNonNull(
            contractIndex, "testPlanExecutionStarted must initialize the contract index first");
    return findContractSimpleName(className, index::containsKey, getClass());
  }

  protected static String findContractSimpleName(
      String className, Predicate<String> isContractName, Class<?> fallbackClass) {
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) cl = fallbackClass.getClassLoader();
      Class<?> clazz = Class.forName(className, false, cl);
      while (clazz != null && clazz != Object.class) {
        if (isContractName.test(clazz.getSimpleName())) {
          return clazz.getSimpleName();
        }
        clazz = clazz.getSuperclass();
      }
    } catch (ClassNotFoundException | NoClassDefFoundError ignored) {
      // Non-fatal: skip this test identifier
    }
    return null;
  }

  /** Display name for this tier (e.g., "Store", "API", "Jakarta Runtime"). */
  protected abstract String tierTitle();

  /**
   * System property name injected by the build tool to identify the runtime under test (e.g.,
   * {@code ratchet.tck.store.name}, {@code ratchet.tck.runtime.name}).
   */
  protected abstract String runtimeProperty();

  /** Path where the report is written (relative to CWD, typically {@code target/}). */
  protected abstract Path reportPath();

  /** Groups of contracts this tier exercises, in display order. */
  protected abstract List<ContractGroup> contractGroups();

  private void writeReport(PrintWriter pw) {
    String runtime = System.getProperty(runtimeProperty(), "unknown");

    pw.printf("# Ratchet TCK %s Conformance Report%n", tierTitle());
    pw.println();
    pw.printf("**Runtime:** %s  %n", runtime);
    pw.printf("**Generated:** %s  %n", Instant.now());
    pw.println();

    // Per-group summary counts: [required, passed]
    Map<String, int[]> summary = new LinkedHashMap<>();
    for (ContractGroup group : contractGroups()) {
      summary.put(group.label(), new int[] {0, 0});
    }

    for (ContractGroup group : contractGroups()) {
      pw.printf("## %s Contracts%n", group.label());
      pw.println();
      pw.println(group.description());
      pw.println();
      pw.println("| Contract | Tests | PASS | FAIL | SKIP | Status |");
      pw.println("|----------|-------|------|------|------|--------|");

      int[] counts = summary.get(group.label());
      for (String contractName : group.contracts()) {
        counts[0]++;
        ContractResult r = results.get(contractName);
        if (r == null) {
          pw.printf("| `%s` | — | — | — | — | ✗ MISSING |%n", contractName);
        } else {
          boolean passed = r.failed == 0 && r.aborted == 0;
          if (passed) counts[1]++;
          pw.printf(
              "| `%s` | %d | %d | %d | %d | %s |%n",
              contractName, r.total(), r.passed, r.failed, r.aborted, passed ? "✓ PASS" : "✗ FAIL");
        }
      }
      pw.println();
    }

    pw.println("## Summary");
    pw.println();
    pw.println("| Group | Required | Passed | Status |");
    pw.println("|-------|----------|--------|--------|");
    boolean allPassed = true;
    for (ContractGroup group : contractGroups()) {
      int[] counts = summary.get(group.label());
      boolean groupPassed = counts[0] == counts[1];
      if (!groupPassed) allPassed = false;
      pw.printf(
          "| %s | %d | %d | %s |%n",
          group.label(), counts[0], counts[1], groupPassed ? "✓ PASS" : "✗ FAIL");
    }
    pw.println();
    if (allPassed) {
      pw.printf("**Conformance Claim: Ratchet %s Compatible**%n", tierTitle());
    } else {
      pw.println("**Conformance Claim: INCOMPLETE — see failing or missing contracts above**");
    }
  }

  /**
   * A named group of related contracts within a tier (e.g., "Core", "Behavioral"). Tiers with few
   * contracts may use a single group.
   */
  public record ContractGroup(String label, String description, List<String> contracts) {}

  /** Result accumulator for a single abstract contract class. */
  public static final class ContractResult {
    public int passed;
    public int failed;
    public int aborted;

    public void record(TestExecutionResult.Status status) {
      switch (status) {
        case SUCCESSFUL -> passed++;
        case FAILED -> failed++;
        case ABORTED -> aborted++;
      }
    }

    public int total() {
      return passed + failed + aborted;
    }
  }
}
