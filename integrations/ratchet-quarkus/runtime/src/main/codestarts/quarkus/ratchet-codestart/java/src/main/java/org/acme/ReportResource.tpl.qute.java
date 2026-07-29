package org.acme;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import run.ratchet.api.JobSchedulerService;

@Path("/reports")
public class ReportResource {
  @Inject JobSchedulerService scheduler;
  @Inject Reports reports;

  @POST
  public String schedule() {
    scheduler.enqueueNow(reports::rebuild);
    return "submitted";
  }
}
