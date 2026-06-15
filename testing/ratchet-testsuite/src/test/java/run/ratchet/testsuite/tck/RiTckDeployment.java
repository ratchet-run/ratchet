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

import org.jboss.shrinkwrap.api.spec.WebArchive;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;
import run.ratchet.tck.util.ConcurrentTestRunner;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

final class RiTckDeployment {

  private RiTckDeployment() {}

  static WebArchive create(Package... additionalPackages) {
    return createWith(additionalPackages);
  }

  static WebArchive createWith(Package[] additionalPackages, Class<?>... additionalClasses) {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    RatchetArchiveBuilder builder =
        RatchetArchiveBuilder.create()
            .addRatchetDependencies(profile, dbType)
            .addPackage(RatchetTckRuntime.class.getPackage())
            .addPackage(ConcurrentTestRunner.class.getPackage())
            .addClasses(RiRatchetTckRuntime.class, ListenerProbe.class, TckJobs.class)
            .addStoreInfrastructure()
            .addBeansXml();

    for (Package pkg : additionalPackages) {
      builder.addPackage(pkg);
    }
    if (additionalClasses.length > 0) {
      builder.addClasses(additionalClasses);
    }

    return builder.build();
  }
}
