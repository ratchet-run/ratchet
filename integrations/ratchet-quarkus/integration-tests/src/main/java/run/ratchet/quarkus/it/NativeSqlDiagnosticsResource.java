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
package run.ratchet.quarkus.it;

import io.quarkus.arc.profile.UnlessBuildProfile;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** SQL-only diagnostics used by the native schema-migration regression test. */
@Path("/jobs")
@UnlessBuildProfile("mongo")
public class NativeSqlDiagnosticsResource {

  @Inject EntityManager appEntityManager;

  @GET
  @Path("/migration-count")
  @Produces(MediaType.TEXT_PLAIN)
  @Transactional
  public String migrationCount() {
    Number count =
        (Number)
            appEntityManager
                .createNativeQuery("SELECT COUNT(*) FROM ratchet_schema_version")
                .getSingleResult();
    return count.toString();
  }
}
