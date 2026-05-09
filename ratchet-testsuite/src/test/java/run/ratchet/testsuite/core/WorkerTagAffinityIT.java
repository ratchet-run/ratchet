package run.ratchet.testsuite.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Verifies that per-node tag affinity filters are applied correctly by the claim store. Two
 * simulated nodes use different tag filters and assert correct, non-overlapping job routing.
 */
class WorkerTagAffinityIT extends BaseRatchetIT {

  @Inject private JobCrudStore jobCrudStore;
  @Inject private JobClaimStore jobClaimStore;
  @Inject private TagStore tagStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void nodeWithRequireTags_onlyClaimsTaggedJobs() {
    Instant due = Instant.now().minusSeconds(5);
    JobEntity gpuJob = persistJob(due, "gpu");
    JobEntity cpuJob = persistJob(due);

    NodeTagFilter gpuNode = new NodeTagFilter(List.of("gpu"), List.of());
    List<JobClaimDto> claimed =
        jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "gpu-node", gpuNode);

    assertEquals(1, claimed.size(), "gpu-node should only claim the gpu-tagged job");
    assertEquals(gpuJob.getId(), claimed.get(0).id());

    List<JobClaimDto> unclaimedCheck =
        jobClaimStore.claimNextBatchOptimized(
            JobExecutionType.SINGLE, 10, "verify-node", NodeTagFilter.NONE);
    assertEquals(1, unclaimedCheck.size(), "cpu job should still be unclaimed");
    assertEquals(cpuJob.getId(), unclaimedCheck.get(0).id());
  }

  @Test
  void nodeWithExcludeTags_skipsTaggedJobs() {
    Instant due = Instant.now().minusSeconds(5);
    persistJob(due, "gpu");
    JobEntity cpuJob = persistJob(due, "cpu");
    JobEntity untaggedJob = persistJob(due);

    NodeTagFilter generalNode = new NodeTagFilter(List.of(), List.of("gpu"));
    List<JobClaimDto> claimed =
        jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "gen-node", generalNode);

    assertEquals(2, claimed.size(), "general node should claim cpu and untagged jobs");
    List<UUID> claimedIds = claimed.stream().map(JobClaimDto::id).toList();
    assertTrue(claimedIds.contains(cpuJob.getId()), "cpu job should be claimed");
    assertTrue(claimedIds.contains(untaggedJob.getId()), "untagged job should be claimed");
  }

  @Test
  void nodeWithRequireAndExcludeTags_claimsRequiredJobsWithoutExcludedTags() {
    Instant due = Instant.now().minusSeconds(5);
    JobEntity currentGpuJob = persistJob(due, "gpu");
    persistJob(due, "gpu", "old-gpu");
    persistJob(due, "cpu");
    persistJob(due);

    NodeTagFilter currentGpuNode = new NodeTagFilter(List.of("gpu"), List.of("old-gpu"));
    List<JobClaimDto> claimed =
        jobClaimStore.claimNextBatchOptimized(
            JobExecutionType.SINGLE, 10, "current-gpu-node", currentGpuNode);

    assertEquals(1, claimed.size(), "node should require gpu while excluding old-gpu");
    assertEquals(currentGpuJob.getId(), claimed.get(0).id());
  }

  @Test
  void twoNodes_routeJobsWithoutDoubleClaimOrDrops() {
    Instant due = Instant.now().minusSeconds(5);
    JobEntity gpuJob = persistJob(due, "gpu");
    JobEntity cpuJob = persistJob(due, "cpu");
    JobEntity untaggedJob = persistJob(due);

    NodeTagFilter nodeA = new NodeTagFilter(List.of("gpu"), List.of());
    NodeTagFilter nodeB = new NodeTagFilter(List.of(), List.of("gpu"));

    List<JobClaimDto> nodeAClaims =
        jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-A", nodeA);
    List<JobClaimDto> nodeBClaims =
        jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-B", nodeB);

    assertEquals(1, nodeAClaims.size(), "node-A should claim only the gpu job");
    assertEquals(gpuJob.getId(), nodeAClaims.get(0).id());

    assertEquals(2, nodeBClaims.size(), "node-B should claim cpu and untagged jobs");
    List<UUID> nodeBIds = nodeBClaims.stream().map(JobClaimDto::id).toList();
    assertTrue(nodeBIds.contains(cpuJob.getId()), "node-B should claim cpu job");
    assertTrue(nodeBIds.contains(untaggedJob.getId()), "node-B should claim untagged job");

    List<UUID> allClaimed =
        java.util.stream.Stream.concat(nodeAClaims.stream(), nodeBClaims.stream())
            .map(JobClaimDto::id)
            .toList();
    assertEquals(
        allClaimed.stream().distinct().count(),
        allClaimed.size(),
        "no job should be claimed by both nodes");
  }

  private JobEntity persistJob(Instant scheduledTime, String... tags) {
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(scheduledTime);
    job.setPayload(JobPayloadFactory.noop());
    job.setIdempotencyKey(UUID.randomUUID().toString());
    JobEntity saved = jobCrudStore.save(job);
    if (tags.length > 0) {
      tagStore.insertTags(saved.getId(), List.of(tags));
    }
    return saved;
  }
}
