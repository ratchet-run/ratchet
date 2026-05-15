package run.ratchet.tck.api;

import java.nio.file.Path;
import java.util.List;
import run.ratchet.tck.util.AbstractConformanceReportExtension;

/**
 * API-tier conformance report listener. Writes {@code target/tck-api-conformance-report.md} after
 * any test run that exercises at least one {@code ratchet-tck-api} contract class.
 *
 * <p>Runtimes inject identity via Failsafe's {@code systemPropertyVariables}: {@code
 * ratchet.tck.runtime.name=${testsuite.profile}/${ratchet.test.db.type}}.
 */
public class ApiConformanceReportExtension extends AbstractConformanceReportExtension {

  private static final List<ContractGroup> GROUPS =
      List.of(
          new ContractGroup(
              "API Behavioral",
              "Pure-JVM behavioral contracts for the JobSchedulerService public API.",
              List.of(
                  "AbstractJobLifecycleContract",
                  "AbstractJobRetryContract",
                  "AbstractJobCancelContract",
                  "AbstractDelayedSchedulingContract",
                  "AbstractIdempotencyContract",
                  "AbstractSimpleWorkflowContract",
                  "AbstractResilienceStrategyContract")));

  @Override
  protected String tierTitle() {
    return "API";
  }

  @Override
  protected String runtimeProperty() {
    return "ratchet.tck.runtime.name";
  }

  @Override
  protected Path reportPath() {
    return Path.of("target", "tck-api-conformance-report.md");
  }

  @Override
  protected List<ContractGroup> contractGroups() {
    return GROUPS;
  }
}
