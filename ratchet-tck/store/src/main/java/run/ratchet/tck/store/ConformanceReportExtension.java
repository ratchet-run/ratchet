package run.ratchet.tck.store;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import run.ratchet.tck.util.AbstractConformanceReportExtension;

/**
 * Store-tier conformance report listener. Extends {@link AbstractConformanceReportExtension} with
 * the 22-contract {@link ConformanceLevel} registry and writes to {@code
 * target/tck-conformance-report.md}.
 *
 * <p>Store modules inject the store identity via Surefire's {@code systemPropertyVariables}: {@code
 * ratchet.tck.store.name=${project.artifactId}}.
 */
public class ConformanceReportExtension extends AbstractConformanceReportExtension {

  private static final String STORE_NAME_PROP = "ratchet.tck.store.name";

  private static final List<ContractGroup> GROUPS =
      Arrays.stream(ConformanceLevel.values())
          .map(l -> new ContractGroup(l.getLabel(), l.getDescription(), l.getRequiredContracts()))
          .toList();

  /**
   * Walks the superclass chain of the given class name looking for a class whose simple name
   * matches a known {@link ConformanceLevel} contract. Kept as a static utility for tests and
   * diagnostic tooling.
   */
  public static String findContractSimpleName(String className) {
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) cl = ConformanceReportExtension.class.getClassLoader();
      Class<?> clazz = Class.forName(className, false, cl);
      while (clazz != null && clazz != Object.class) {
        if (ConformanceLevel.forContract(clazz.getSimpleName()) != null) {
          return clazz.getSimpleName();
        }
        clazz = clazz.getSuperclass();
      }
    } catch (ClassNotFoundException | NoClassDefFoundError ignored) {
      // Non-fatal: skip
    }
    return null;
  }

  @Override
  protected String tierTitle() {
    return "Store";
  }

  @Override
  protected String runtimeProperty() {
    return STORE_NAME_PROP;
  }

  @Override
  protected Path reportPath() {
    return Path.of("target", "tck-conformance-report.md");
  }

  @Override
  protected List<ContractGroup> contractGroups() {
    return GROUPS;
  }
}
