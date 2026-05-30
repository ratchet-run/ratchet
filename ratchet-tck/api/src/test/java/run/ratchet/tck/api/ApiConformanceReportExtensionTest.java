package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.util.AbstractConformanceReportExtension.ContractGroup;

class ApiConformanceReportExtensionTest {

  /**
   * Every shipped {@code Abstract*Contract} in the API tier must appear in the report catalog.
   * Missing-contract detection and the final "Ratchet API Compatible" verdict are driven entirely
   * by the catalog, so a contract left out runs but never counts toward PASS/FAIL accounting. The
   * scan is anchored on a contract class's own resource so it reads the main output directory where
   * the contracts live, not the test-classes copy of the package.
   */
  @Test
  void allApiContractsAreCataloged() throws Exception {
    var resource =
        AbstractJobLifecycleContract.class.getResource("AbstractJobLifecycleContract.class");
    var packageDir = Paths.get(resource.toURI()).getParent();

    Set<String> cataloged =
        new ApiConformanceReportExtension()
            .contractGroups().stream()
                .flatMap(
                    (ContractGroup g) ->
                        Stream.concat(g.contracts().stream(), g.optionalContracts().stream()))
                .collect(Collectors.toSet());

    List<String> uncataloged;
    try (var classes = Files.list(packageDir)) {
      uncataloged =
          classes
              .map(path -> path.getFileName().toString())
              .filter(name -> name.startsWith("Abstract"))
              .filter(name -> name.endsWith("Contract.class"))
              .map(name -> name.substring(0, name.length() - ".class".length()))
              .filter(name -> !cataloged.contains(name))
              .sorted()
              .toList();
    }

    assertEquals(List.of(), uncataloged, "all API Abstract*Contract classes are cataloged");
  }
}
