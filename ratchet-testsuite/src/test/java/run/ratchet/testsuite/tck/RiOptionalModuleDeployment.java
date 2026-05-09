package run.ratchet.testsuite.tck;

import java.io.File;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

final class RiOptionalModuleDeployment {

  private RiOptionalModuleDeployment() {}

  static WebArchive create(String artifact) {
    return create(artifact, new Package[0]);
  }

  static WebArchive create(String artifact, Package[] packages, Class<?>... classes) {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    File[] optionalModuleJars =
        Maven.resolver().loadPomFromFile("pom.xml").resolve(artifact).withTransitivity().asFile();

    RatchetArchiveBuilder builder =
        RatchetArchiveBuilder.create()
            .addRatchetDependencies(profile, dbType)
            .addStoreInfrastructure()
            .addBeansXml();
    for (Package pkg : packages) {
      builder.addPackage(pkg);
    }
    if (classes.length > 0) {
      builder.addClasses(classes);
    }
    return builder.build().addAsLibraries(optionalModuleJars);
  }
}
