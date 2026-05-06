package run.ratchet.testsuite.util;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.Set;
import run.ratchet.ri.security.PackagePrefixClassPolicy;
import run.ratchet.spi.ClassPolicy;

/**
 * Test-scoped {@link ClassPolicy} that allows all Ratchet classes to be executed as job targets.
 *
 * <p>This overrides the default empty-allowlist {@link PackagePrefixClassPolicy} from {@code
 * RatchetProducer} so that test job lambdas (e.g. {@code SimpleJob::execute}) pass the security
 * check. Uses {@code @Alternative @Priority} to take precedence over the default producer.
 */
@ApplicationScoped
@Alternative
@Priority(1)
public class TestClassPolicy extends PackagePrefixClassPolicy implements ClassPolicy {

  public TestClassPolicy() {
    super(Set.of("run.ratchet."));
  }
}
