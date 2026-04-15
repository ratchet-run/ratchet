package run.ratchet.loadtest.resource;

import run.ratchet.loadtest.api.ResetRequest;
import run.ratchet.loadtest.api.RunStartedResponse;
import run.ratchet.loadtest.api.StartRunRequest;
import run.ratchet.loadtest.service.LoadTestResetService;
import run.ratchet.loadtest.service.LoadTestRunner;
import run.ratchet.loadtest.service.RunMetadata;
import run.ratchet.loadtest.service.RunStatusService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/api")
@ApplicationScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LoadTestResource {

  @Inject LoadTestRunner runner;
  @Inject RunStatusService statusService;
  @Inject LoadTestResetService resetService;

  @POST
  @Path("/runs")
  public RunStartedResponse start(StartRunRequest request) {
    try {
      RunMetadata metadata = runner.start(request);
      return new RunStartedResponse(
          metadata.runId(), metadata.workload(), metadata.expectedJobs(), metadata.startedAt());
    } catch (RuntimeException e) {
      throw badRequest(e);
    }
  }

  @GET
  @Path("/runs/{runId}")
  public Object status(@PathParam("runId") String runId) {
    return statusService.status(runId);
  }

  @GET
  @Path("/cluster")
  public Object cluster() {
    return statusService.cluster();
  }

  @POST
  @Path("/reset")
  public Object reset(ResetRequest request) {
    String runId = request == null ? null : request.runId;
    return resetService.reset(runId);
  }

  private static WebApplicationException badRequest(RuntimeException e) {
    return new WebApplicationException(
        Response.status(Response.Status.BAD_REQUEST)
            .type(MediaType.TEXT_PLAIN_TYPE)
            .entity(e.getMessage())
            .build());
  }
}
