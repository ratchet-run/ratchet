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
package run.ratchet.loadtest.resource;

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
import java.time.Instant;
import run.ratchet.loadtest.api.EnqueueJobRequest;
import run.ratchet.loadtest.api.JobEnqueuedResponse;
import run.ratchet.loadtest.api.NodeResponse;
import run.ratchet.loadtest.api.ResetRequest;
import run.ratchet.loadtest.api.RunStartedResponse;
import run.ratchet.loadtest.api.StartRunRequest;
import run.ratchet.loadtest.service.LoadTestResetService;
import run.ratchet.loadtest.service.LoadTestRunner;
import run.ratchet.loadtest.service.RunMetadata;
import run.ratchet.loadtest.service.RunStatusService;
import run.ratchet.spi.NodeIdentityProvider;

@Path("/api")
@ApplicationScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LoadTestResource {

  @Inject LoadTestRunner runner;
  @Inject RunStatusService statusService;
  @Inject LoadTestResetService resetService;
  @Inject NodeIdentityProvider nodeIdentityProvider;

  private static WebApplicationException badRequest(RuntimeException e) {
    String message = e.getMessage();
    if (message == null || message.isBlank()) {
      message = e.getClass().getSimpleName();
    }
    return new WebApplicationException(
        Response.status(Response.Status.BAD_REQUEST)
            .type(MediaType.TEXT_PLAIN_TYPE)
            .entity(message)
            .build());
  }

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

  @POST
  @Path("/jobs")
  public Response enqueue(EnqueueJobRequest request) {
    try {
      JobEnqueuedResponse response = runner.enqueue(request);
      return Response.accepted(response)
          .header("X-Ratchet-Node-Id", response.acceptedNodeId)
          .build();
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
  @Path("/node")
  public Response node() {
    NodeResponse response = new NodeResponse(nodeIdentityProvider.getNodeId(), Instant.now());
    return Response.ok(response).header("X-Ratchet-Node-Id", response.getNodeId()).build();
  }

  @GET
  @Path("/cluster")
  public Object cluster() {
    return statusService.cluster();
  }

  @POST
  @Path("/reset")
  public Object reset(ResetRequest request) {
    String runId = request == null ? null : request.getRunId();
    return resetService.reset(runId);
  }
}
