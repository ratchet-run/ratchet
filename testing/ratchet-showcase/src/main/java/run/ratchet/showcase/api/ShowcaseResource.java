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
package run.ratchet.showcase.api;

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
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobSummary;
import run.ratchet.api.QueueHealthSnapshot;
import run.ratchet.api.SignalDecision;
import run.ratchet.showcase.domain.OrderStatus;
import run.ratchet.showcase.domain.RecentJob;
import run.ratchet.showcase.domain.ReviewTicket;
import run.ratchet.showcase.jobs.ShowcaseJobs;
import run.ratchet.showcase.service.OrderRepository;
import run.ratchet.showcase.service.OrderScenarioService;
import run.ratchet.showcase.service.OrderStreamService;
import run.ratchet.showcase.service.OrderWorkflowService;
import run.ratchet.showcase.service.ShowcaseCircuitBreakers;
import run.ratchet.showcase.service.ShowcaseJobCleaner;

@Path("/api")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ShowcaseResource {

  @Inject OrderStreamService stream;
  @Inject OrderScenarioService scenarios;
  @Inject OrderWorkflowService workflow;
  @Inject OrderRepository repository;
  @Inject JobQueryService jobQuery;
  @Inject JobSchedulerService scheduler;
  @Inject ShowcaseCircuitBreakers circuitBreakers;
  @Inject ShowcaseJobCleaner cleaner;

  @GET
  @Path("/runtime")
  public Map<String, Object> runtime() {
    Map<String, Object> runtime = new LinkedHashMap<>();
    runtime.put("serverProfile", value("SHOWCASE_SERVER", "showcase.server", "unknown"));
    runtime.put("dbProfile", value("SHOWCASE_DB", "showcase.db", "unknown"));
    runtime.put("nodeId", ManagementFactory.getRuntimeMXBean().getName());
    runtime.put("appVersion", value("SHOWCASE_VERSION", "showcase.version", "dev"));
    runtime.put("paymentOutageUntil", workflow.paymentOutageUntil());
    runtime.put("activeResources", List.of("warehouse-robots", "payment-gateway", "carrier-api"));
    return runtime;
  }

  @GET
  @Path("/dashboard")
  public Map<String, Object> dashboard() {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("stream", repository.streamState());
    snapshot.put("orders", repository.recentOrders(25));
    snapshot.put("statusCounts", statusCounts());
    snapshot.put("reviews", repository.openReviews());
    snapshot.put("burst", repository.burstProgress());
    snapshot.put("queueHealth", queueHealth());
    snapshot.put("recentJobs", recentJobs());
    return snapshot;
  }

  @POST
  @Path("/stream/start")
  public Object startStream(StreamStartRequest request) {
    StreamStartRequest body = request == null ? new StreamStartRequest() : request;
    circuitBreakers.resetAll();
    return stream.start(body.seed, body.ordersPerMinute, body.burstiness, body.failureMix);
  }

  @POST
  @Path("/stream/update")
  public Object updateStream(StreamUpdateRequest request) {
    StreamUpdateRequest body = request == null ? new StreamUpdateRequest() : request;
    return stream.update(body.ordersPerMinute, body.seed, body.burstiness, body.failureMix);
  }

  @POST
  @Path("/stream/stop")
  public Object stopStream() {
    return stream.stop();
  }

  @POST
  @Path("/scenarios/import-burst")
  public Map<String, Object> importBurst(ImportBurstRequest request) {
    ImportBurstRequest body = request == null ? new ImportBurstRequest() : request;
    circuitBreakers.resetAll();
    JobHandle handle = scenarios.importBurst(body.count, body.seed);
    return Map.of("batchJobId", handle.id().toString());
  }

  @POST
  @Path("/scenarios/payment-outage")
  public Map<String, Object> paymentOutage(PaymentOutageRequest request) {
    PaymentOutageRequest body = request == null ? new PaymentOutageRequest() : request;
    circuitBreakers.resetAll();
    Instant until = scenarios.paymentOutage(body.seconds);
    List<JobHandle> handles = scenarios.paymentOutageTraffic(body.count);
    Map<String, Object> response = scenarioResponse("payment-outage", handles);
    response.put("paymentOutageUntil", until.toString());
    return response;
  }

  @POST
  @Path("/scenarios/fraud-review")
  public Map<String, Object> fraudReview() {
    circuitBreakers.resetAll();
    return scenarioResponse("fraud-review", scenarios.fraudReview());
  }

  @POST
  @Path("/scenarios/bad-card")
  public Map<String, Object> badCard() {
    circuitBreakers.resetAll();
    return scenarioResponse("bad-card", scenarios.badCard());
  }

  @POST
  @Path("/scenarios/warehouse-crunch")
  public Map<String, Object> warehouseCrunch(ImportBurstRequest request) {
    ImportBurstRequest body = request == null ? new ImportBurstRequest() : request;
    circuitBreakers.resetAll();
    return scenarioResponse("warehouse-crunch", scenarios.warehouseCrunch(body.count));
  }

  @POST
  @Path("/scenarios/carrier-outage")
  public Map<String, Object> carrierOutage(ImportBurstRequest request) {
    ImportBurstRequest body = request == null ? new ImportBurstRequest() : request;
    circuitBreakers.resetAll();
    return scenarioResponse("carrier-outage", scenarios.carrierOutage(body.count));
  }

  @POST
  @Path("/reviews/{id}/decision")
  public Map<String, Object> reviewDecision(
      @PathParam("id") String reviewId, DecisionRequest request) {
    DecisionRequest body = request == null ? new DecisionRequest() : request;
    boolean approve = body.decision == null || "approve".equalsIgnoreCase(body.decision);
    String decisionLabel = approve ? "APPROVE" : "REJECT";
    String reason = body.reason == null && !approve ? "Rejected" : body.reason;
    SignalDecision decision =
        approve ? SignalDecision.approved(reviewId) : SignalDecision.rejected(reviewId, reason);
    int delivered = scheduler.deliverSignal(reviewId, decision);
    if (delivered > 0) {
      repository.decideReview(reviewId, decisionLabel, reason);
      return reviewDecisionResponse(reviewId, delivered, decisionLabel, "signal");
    }
    String handled =
        repository
            .review(reviewId)
            .filter(ReviewTicket::isOpen)
            .map(review -> completeLateReviewDecision(review, approve, reason))
            .orElse("stale");
    return reviewDecisionResponse(reviewId, delivered, decisionLabel, handled);
  }

  private String completeLateReviewDecision(ReviewTicket review, boolean approve, String reason) {
    if (approve) {
      repository.decideReview(review.id, "APPROVE", reason);
      repository.transition(
          review.orderId, OrderStatus.REVIEW_APPROVED, "Fraud review approved after signal wait");
      workflow.startFulfillment(review.orderId);
      return "manual";
    }
    repository.decideReview(review.id, "REJECT", reason);
    repository.transition(
        review.orderId,
        OrderStatus.REVIEW_REJECTED,
        reason == null ? "Dashboard decision rejected" : reason);
    return "manual";
  }

  private static Map<String, Object> reviewDecisionResponse(
      String reviewId, int delivered, String decision, String handled) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("reviewId", reviewId);
    response.put("delivered", delivered);
    response.put("decision", decision);
    response.put("handled", handled);
    return response;
  }

  private static Map<String, Object> scenarioResponse(String scenario, JobHandle handle) {
    return scenarioResponse(scenario, List.of(handle));
  }

  private static Map<String, Object> scenarioResponse(String scenario, List<JobHandle> handles) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("scenario", scenario);
    response.put("submitted", handles.size());
    response.put("jobIds", handles.stream().map(handle -> handle.id().toString()).toList());
    return response;
  }

  @POST
  @Path("/jobs/{id}/retry")
  public Map<String, Object> retry(@PathParam("id") String jobId) {
    boolean retried = scheduler.retryJob(parseUuid(jobId));
    return Map.of("jobId", jobId, "retried", retried);
  }

  @POST
  @Path("/reset")
  public Map<String, Object> reset() {
    ShowcaseJobCleaner.CleanupResult result = cleaner.resetForFreshRun();
    return Map.of(
        "cancelledJobs", result.activeByTag(),
        "cancelledRecurring", result.recurringByTag() + result.recurringByBusinessKey(),
        "deletedJobs", result.deletedByTarget() + result.deletedByTag());
  }

  private Map<String, Long> statusCounts() {
    Map<String, Long> counts = new LinkedHashMap<>();
    EnumMap<OrderStatus, Long> typed = new EnumMap<>(repository.statusCounts());
    for (OrderStatus status : OrderStatus.values()) {
      counts.put(status.name(), typed.getOrDefault(status, 0L));
    }
    return counts;
  }

  private Map<String, Object> queueHealth() {
    QueueHealthSnapshot health = jobQuery.getQueueHealth();
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("pending", health.pendingCount());
    value.put("running", health.runningCount());
    value.put("failed", health.failedCount());
    value.put("succeeded", health.succeededCount());
    value.put("canceled", health.canceledCount());
    value.put("paused", health.pausedCount());
    value.put("waiting", health.waitingCount());
    value.put("ready", health.readyCount());
    value.put("retryRate", health.retryRate());
    value.put("p95QueueWaitMs", health.p95QueueWaitMs());
    value.put("oldestPendingJobTime", health.oldestPendingJobTime());
    return value;
  }

  private List<RecentJob> recentJobs() {
    Map<String, RecentJob> jobs = new LinkedHashMap<>();
    addRecentJobs(
        jobs,
        JobFilter.builder()
            .targetClass(ShowcaseJobs.class.getName())
            .statuses(JobStatus.FAILED, JobStatus.WAITING)
            .sortField(JobQuerySortField.UPDATED_AT)
            .sortAscending(false)
            .skipCount(true)
            .build(),
        10);
    addRecentJobs(
        jobs,
        JobFilter.builder()
            .targetClass(ShowcaseJobs.class.getName())
            .sortField(JobQuerySortField.UPDATED_AT)
            .sortAscending(false)
            .skipCount(true)
            .build(),
        30);
    return jobs.values().stream().limit(30).toList();
  }

  private void addRecentJobs(Map<String, RecentJob> jobs, JobFilter filter, int limit) {
    jobQuery.findJobs(filter, limit, 0).items().stream()
        .map(ShowcaseResource::recentJob)
        .forEach(job -> jobs.putIfAbsent(job.id, job));
  }

  private static RecentJob recentJob(JobSummary summary) {
    RecentJob job = new RecentJob();
    job.id = summary.id().toString();
    job.status = summary.status().name();
    job.type = summary.type().name();
    job.priority = summary.priority().name();
    job.target = summary.targetClass();
    job.method = summary.methodName();
    job.businessKey = summary.businessKey();
    job.resource = summary.resourceName();
    job.pickedBy = summary.pickedBy();
    job.error = summary.lastError();
    job.attempts = summary.attempts();
    job.maxRetries = summary.maxRetries();
    job.tags = summary.tags();
    job.createdAt = summary.createdAt();
    job.scheduledTime = summary.scheduledTime();
    job.updatedAt = summary.updatedAt();
    job.dependsOn = summary.dependsOn();
    return job;
  }

  private static UUID parseUuid(String value) {
    try {
      return UUID.fromString(value);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException("Invalid job id: " + value, Response.Status.BAD_REQUEST);
    }
  }

  private static String value(String envName, String propertyName, String fallback) {
    String env = System.getenv(envName);
    if (env != null && !env.isBlank()) {
      return env;
    }
    String property = System.getProperty(propertyName);
    return property == null || property.isBlank() ? fallback : property;
  }
}
