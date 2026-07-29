package run.ratchet.quarkus.codestart;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

class CodestartPackagingTest {

    @Test
    void stagedCodestartIncludesConcreteRatchetStoreVersion() throws IOException {
        Path codestart = Paths.get(
                "target",
                "codestarts-staging",
                "codestarts",
                "quarkus",
                "ratchet-codestart",
                "codestart.yml");

        assertTrue(
                Files.exists(codestart),
                () -> "Expected staged codestart.yml to exist at " + codestart.toAbsolutePath());

        String text = Files.readString(codestart);
        String dependencyLine = text.lines()
                .filter(line -> line.contains("run.ratchet:ratchet-store-postgresql"))
                .findFirst()
                .orElse("");

        assertFalse(
                dependencyLine.isEmpty(),
                "Expected ratchet-store-postgresql dependency in staged codestart.yml");
        assertFalse(
                dependencyLine.matches(".*run\\.ratchet:ratchet-store-postgresql\\s*$"),
                "Expected ratchet-store-postgresql dependency to include a version, but found bare dependency: "
                        + dependencyLine);
        assertTrue(
                dependencyLine.matches(".*run\\.ratchet:ratchet-store-postgresql:[^\\s]+.*"),
                "Expected ratchet-store-postgresql dependency to include a concrete version, but found: "
                        + dependencyLine);
        assertFalse(
                text.contains("${"),
                "Expected staged codestart.yml to have no unfiltered Maven placeholders");
    }
}
