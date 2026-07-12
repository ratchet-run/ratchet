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
package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
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
    job.setParams(Map.of("tenant", "acme"));
    job.setTags(List.of("billing", "nightly"));
    job.setBusinessKey("order-1");
    job.setIdempotencyKey("idempotency-1");
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
    JobEntity reloaded = DocumentMapper.toJobEntity(doc);

    assertInstanceOf(String.class, doc.get("payload"));
    assertEquals(payload, reloaded.getPayload());
    assertEquals(job.getId(), reloaded.getId());
    assertEquals(JobStatus.PENDING, reloaded.getStatus());
    assertEquals(JobExecutionType.SINGLE, reloaded.getJobType());
    assertEquals(JobPriority.NORMAL, reloaded.getPriority());
    assertEquals(job.getScheduledTime(), reloaded.getScheduledTime());
    assertEquals(job.getTags(), reloaded.getTags());
    assertEquals(job.getParams(), reloaded.getParams());
    assertEquals("order-1", reloaded.getBusinessKey());
  }

  @Test
  void writesExistingJobDefaultsForNullDefaultedFields() {
    JobEntity job = job(null);
    job.setStatus(null);
    job.setCronExpr(null);
    job.setZoneId(null);
    job.setTags(null);
    job.setVersion(null);

    Document doc = DocumentMapper.toDocument(job);

    assertEquals("PENDING", doc.getString("status"));
    assertEquals("", doc.getString("cron_expr"));
    assertEquals("UTC", doc.getString("zone_id"));
    assertEquals(List.of(), doc.getList("tags", String.class));
    assertEquals(0, doc.getInteger("version"));
  }

  @Test
  void readsMissingOrInvalidJobPriorityAsNormal() {
    Document doc = DocumentMapper.toDocument(job(null));
    doc.remove("priority");

    assertEquals(JobPriority.NORMAL, DocumentMapper.toJobEntity(doc).getPriority());

    doc.put("priority", -1);
    assertEquals(JobPriority.NORMAL, DocumentMapper.toJobEntity(doc).getPriority());
  }

  @Test
  void rejectsMissingRequiredEnumWithFieldContext() {
    Document doc = DocumentMapper.toDocument(job(null));
    doc.remove("status");

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> DocumentMapper.toJobEntity(doc));

    assertEquals("Required enum field 'status' is null for JobStatus", thrown.getMessage());
  }

  @Test
  void readsTraceContextWithoutUncheckedCasts() {
    JobEntity job = job(payload("com.example.TraceJob", "run"));
    job.setTraceContext(Map.of("traceparent", "00-abc-def-01"));

    JobEntity reloaded = DocumentMapper.toJobEntity(DocumentMapper.toDocument(job));

    assertEquals(job.getTraceContext(), reloaded.getTraceContext());
  }

  @Test
  void workflowConditionRoundTripsDefinitionOrderAndDefaultsLegacyDocumentsToZero() {
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setId(UuidV7Factory.create());
    condition.setParentJobId(UUID.randomUUID());
    condition.setChildJobId(UUID.randomUUID());
    condition.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    condition.setConditionPriority(5);
    condition.setDefinitionOrder(3);
    condition.setCreatedAt(Instant.parse("2026-07-12T00:00:00Z"));

    Document document = DocumentMapper.toDocument(condition);
    assertEquals(3, DocumentMapper.toWorkflowConditionEntity(document).getDefinitionOrder());

    document.remove("definition_order");
    assertEquals(
        0,
        DocumentMapper.toWorkflowConditionEntity(document).getDefinitionOrder(),
        "workflow documents written before definition order should use the legacy fallback");
  }

  @Test
  void readsLegacyDocumentJobPayloads() {
    JobEntity job = job(null);
    JobPayload payload = payload("com.example.LegacyJob", "execute");
    Document doc = DocumentMapper.toDocument(job);
    doc.put("payload", legacyPayloadDocument(payload));

    assertEquals(payload, DocumentMapper.toJobEntity(doc).getPayload());
  }

  @Test
  void readsMissingAndEmptyJobPayloadsAsNull() {
    Document missing = DocumentMapper.toDocument(job(null));
    missing.remove("payload");

    Document empty = DocumentMapper.toDocument(job(null));
    empty.put("payload", "");

    assertNull(DocumentMapper.toJobEntity(missing).getPayload());
    assertNull(DocumentMapper.toJobEntity(empty).getPayload());
  }

  @Test
  void wrapsUnsupportedPayloadStorageTypeWithMappingException() {
    Document doc = DocumentMapper.toDocument(job(null));
    doc.put("payload", 42);

    DocumentMapper.MappingException thrown =
        assertThrows(DocumentMapper.MappingException.class, () -> DocumentMapper.toJobEntity(doc));

    assertTrue(thrown.getMessage().contains("Unsupported MongoDB job payload type"));
    assertTrue(thrown.getMessage().contains("Integer"));
    assertFalse(thrown.getMessage().contains("java.lang.Integer"));
  }

  @Test
  void wrapsMalformedSerializedPayloadWithMappingException() {
    Document doc = DocumentMapper.toDocument(job(null));
    doc.put("payload", "{not-json");

    DocumentMapper.MappingException thrown =
        assertThrows(DocumentMapper.MappingException.class, () -> DocumentMapper.toJobEntity(doc));

    assertTrue(thrown.getMessage().contains("Could not deserialize MongoDB job payload"));
  }

  @Test
  void rejectsMalformedLegacyPayloadArgs() {
    JobPayload payload = payload("com.example.LegacyJob", "execute");
    Document legacyPayload = legacyPayloadDocument(payload);
    legacyPayload.put("args", "not-a-list");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> DocumentMapper.storedValueToPayload(legacyPayload));

    assertEquals(
        "Expected MongoDB payload args list, got: class java.lang.String", thrown.getMessage());
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

  @Test
  void roundTripsJobLogDocuments() {
    JobLogEntity logEntry =
        new JobLogEntity(
            UuidV7Factory.create(),
            Instant.parse("2026-01-01T00:00:00Z"),
            JobLogEntity.LogLevel.WARN,
            "retry scheduled",
            Map.of("jobType", "billing"));
    logEntry.setId(UuidV7Factory.create());

    JobLogEntity reloaded = DocumentMapper.toJobLogEntity(DocumentMapper.toDocument(logEntry));

    assertEquals(logEntry.getId(), reloaded.getId());
    assertEquals(logEntry.getJobId(), reloaded.getJobId());
    assertEquals(logEntry.getTs(), reloaded.getTs());
    assertEquals(logEntry.getLevel(), reloaded.getLevel());
    assertEquals(logEntry.getMessage(), reloaded.getMessage());
    assertEquals(logEntry.getMdc(), reloaded.getMdc());
  }

  @Test
  void roundTripsResourcePermitDocuments() {
    ResourcePermitEntity permit =
        ResourcePermitEntity.create("gpu", UuidV7Factory.create(), "node");
    permit.setId(UuidV7Factory.create());
    permit.setAcquiredAt(Instant.parse("2026-01-01T00:00:00Z"));

    ResourcePermitEntity reloaded =
        DocumentMapper.toResourcePermitEntity(DocumentMapper.toDocument(permit));

    assertEquals(permit.getId(), reloaded.getId());
    assertEquals(permit.getResourceName(), reloaded.getResourceName());
    assertEquals(permit.getJobId(), reloaded.getJobId());
    assertEquals(permit.getNodeId(), reloaded.getNodeId());
    assertEquals(permit.getAcquiredAt(), reloaded.getAcquiredAt());
  }
}
