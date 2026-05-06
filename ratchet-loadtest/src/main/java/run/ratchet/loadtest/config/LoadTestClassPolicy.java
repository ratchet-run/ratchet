package run.ratchet.loadtest.config;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.Set;
import run.ratchet.ri.security.PackagePrefixClassPolicy;

@Alternative
@Priority(2000)
@ApplicationScoped
public class LoadTestClassPolicy extends PackagePrefixClassPolicy {

  public LoadTestClassPolicy() {
    super(Set.of("run.ratchet.loadtest"));
  }
}
