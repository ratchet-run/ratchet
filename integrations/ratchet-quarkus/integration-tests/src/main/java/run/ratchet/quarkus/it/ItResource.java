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

import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.quarkus.it.app.DemoNote;

/**
 * Drives the demo app over HTTP so {@link RatchetQuarkusSmokeTest} ({@code @QuarkusTest}) can
 * exercise the extension against a real Quarkus boot. The one-off job is submitted from
 * <em>inside</em> the application process, exactly as an application would.
 */
@Path("/jobs")
public class ItResource {

  @Inject JobSchedulerService scheduler;
  @Inject ItJobs jobs;

  /**
   * The application's own EntityManager — unqualified, so it resolves to the DEFAULT persistence
   * unit (Ratchet's stores use the {@code @PersistenceUnit("ratchet")} unit via the extension).
   * Writing through it proves the app's unit works alongside Ratchet's.
   */
  @Inject EntityManager appEntityManager;

  @POST
  @Path("/submit")
  @Produces(MediaType.TEXT_PLAIN)
  public String submit() {
    scheduler.enqueueNow(jobs::recordRun);
    return "submitted";
  }

  @GET
  @Path("/executed")
  @Produces(MediaType.TEXT_PLAIN)
  public String executed() {
    return Boolean.toString(jobs.hasExecuted());
  }

  @GET
  @Path("/recurring-executed")
  @Produces(MediaType.TEXT_PLAIN)
  public String recurringExecuted() {
    return Boolean.toString(jobs.hasRecurringExecuted());
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
    long count =
        appEntityManager.createQuery("select count(n) from DemoNote n", Long.class).getSingleResult();
    return Long.toString(count);
  }
}
