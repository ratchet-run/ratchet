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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import run.ratchet.quarkus.it.app.DemoNote;

/** SQL-only smoke endpoint for the app's default persistence unit. */
@Path("/jobs")
@UnlessBuildProfile("mongo")
public class SqlItResource {

  /**
   * The application's own EntityManager, unqualified so it resolves to the default persistence
   * unit. Ratchet's SQL stores use the named {@code "ratchet"} unit through the extension.
   */
  @Inject EntityManager appEntityManager;

  /**
   * Reports whether Ratchet's dependency-provided mapping leaked into the application's default
   * persistence unit.
   */
  @GET
  @Path("/default-unit-has-ratchet-entities")
  @Produces(MediaType.TEXT_PLAIN)
  public String defaultUnitHasRatchetEntities() {
    boolean found =
        appEntityManager.getMetamodel().getEntities().stream()
            .anyMatch(
                entity -> entity.getJavaType().getName().startsWith("run.ratchet.store.entity."));
    return Boolean.toString(found);
  }

  /**
   * Persists an application entity through the default persistence unit and returns the row count.
   * Confirms the app's own unit coexists with Ratchet's named unit on the same datasource.
   */
  @POST
  @Path("/notes")
  @Produces(MediaType.TEXT_PLAIN)
  @Transactional
  public String saveNote() {
    DemoNote note = new DemoNote();
    note.id = 1L;
    note.text = "stored via the application's own persistence unit";
    appEntityManager.merge(note);
    long count = appEntityManager.createNamedQuery("DemoNote.count", Long.class).getSingleResult();
    return Long.toString(count);
  }
}
