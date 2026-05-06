package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.time.Instant;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.id.UuidV7Factory;

class DocumentMapperTest {

  private static JobEntity job(JobPayload payload) {
    JobEntity job = new JobEntity();
    job.setId(UuidV7Factory.create());
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.parse("2026-01-01T00:00:00Z"));
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setAttempts(0);
    job.setMaxRetries(0);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setBackoffParamMs(0);
    job.setTimeoutSec(0);
    job.setCronExpr("");
    job.setZoneId("UTC");
    job.setPayload(payload);
    job.setTags(List.of());
    job.setCreatedAt(Instant.parse("2026-01-01T00:00:00Z"));
    job.setUpdatedAt(Instant.parse("2026-01-01T00:00:00Z"));
    job.setVersion(0);
    return job;
  }

  private static JobPayload payload(String target, String method) {
    return new JobPayload(target, method, "()V", true, List.of());
  }

  private static Document legacyPayloadDocument(JobPayload payload) {
    return new Document("target", payload.target())
        .append("method", payload.method())
        .append("methodDescriptor", payload.methodDescriptor())
        .append("isStatic", payload.isStatic())
        .append("args", payload.args());
  }

  @Test
  void storesJobPayloadsAsSharedJsonStrings() {
    JobPayload payload = payload("com.example.Job", "run");
    JobEntity job = job(payload);

    Document doc = DocumentMapper.toDocument(job);

    assertInstanceOf(String.class, doc.get("payload"));
    assertEquals(payload, DocumentMapper.toJobEntity(doc).getPayload());
  }

  @Test
  void readsLegacyDocumentJobPayloads() {
    JobPayload payload = payload("com.example.LegacyJob", "execute");
    Document doc = DocumentMapper.toDocument(job(null));
    doc.put("payload", legacyPayloadDocument(payload));

    assertEquals(payload, DocumentMapper.toJobEntity(doc).getPayload());
  }

  @Test
  void roundTripsSignalDecisionMetadata() {
    JobEntity job = job(payload("com.example.SignalJob", "run"));
    job.setSignalKey("approval");
    job.setSignalPayload("{\"outcome\":\"REJECTED\"}");
    job.setSignalPayloadType("DECISION");
    job.setSignalOutcome("REJECTED");
    job.setSignalRejectionReason("policy denied");
    job.setSignalDeliveredBy("admin");
    job.setSignalDeliveredAt(Instant.parse("2026-01-01T00:01:00Z"));
    job.setSignalDeliveryId("delivery-id");

    JobEntity reloaded = DocumentMapper.toJobEntity(DocumentMapper.toDocument(job));

    assertEquals("approval", reloaded.getSignalKey());
    assertEquals("{\"outcome\":\"REJECTED\"}", reloaded.getSignalPayload());
    assertEquals("DECISION", reloaded.getSignalPayloadType());
    assertEquals("REJECTED", reloaded.getSignalOutcome());
    assertEquals("policy denied", reloaded.getSignalRejectionReason());
    assertEquals("admin", reloaded.getSignalDeliveredBy());
    assertEquals(Instant.parse("2026-01-01T00:01:00Z"), reloaded.getSignalDeliveredAt());
    assertEquals("delivery-id", reloaded.getSignalDeliveryId());
  }

  @Test
  void storesBatchProgressHookAsSharedJsonStringAndReadsLegacyDocumentShape() {
    JobPayload payload = payload("com.example.Progress", "tick");
    BatchEntity batch = new BatchEntity();
    batch.setId(UuidV7Factory.create());
    batch.setTotalItems(3);
    batch.setCompletedItems(1);
    batch.setFailedItems(0);
    batch.setCompletionProcessed(false);
    batch.setProgressHook(payload);

    Document doc = DocumentMapper.toDocument(batch);
    assertInstanceOf(String.class, doc.get("progress_hook"));
    assertEquals(payload, DocumentMapper.toBatchEntity(doc).getProgressHook());

    doc.put("progress_hook", legacyPayloadDocument(payload));
    assertEquals(payload, DocumentMapper.toBatchEntity(doc).getProgressHook());
  }
}
