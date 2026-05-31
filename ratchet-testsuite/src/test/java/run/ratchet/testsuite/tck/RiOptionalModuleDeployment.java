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
