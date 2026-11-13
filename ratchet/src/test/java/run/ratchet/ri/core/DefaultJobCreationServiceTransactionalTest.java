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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import jakarta.transaction.Transactional.TxType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import run.ratchet.api.TransactionalJob;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.JobInvocation;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.TransactionalCompletionStore;
import run.ratchet.store.spi.WorkflowConditionStore;

class DefaultJobCreationServiceTransactionalTest {
  @Test
  void missingCapabilityRejectsAnnotatedJobBeforePersistence() {
    JobStore store = capableShapeStore();
    DefaultJobCreationService service = service(store, name -> true);
    assertThrows(UnsupportedOperationException.class, () -> submit(service, AnnotatedJob.class));
    verify(store, never()).create(any());
  }

  @Test
  void unannotatedJobDoesNotRequireCapability() {
    JobStore store = capableShapeStore();
    preparePersistence(store);
    assertDoesNotThrow(() -> submit(service(store, name -> true), PlainJob.class));
    verify(store).create(any());
  }

  @Test
  void classPolicyRejectsBeforeMetadataClassLoad() {
    JobStore store = capableShapeStore();
    DefaultJobCreationService service = service(store, name -> false);
    assertThrows(SecurityException.class, () -> submit(service, AnnotatedJob.class));
    verify(store, never()).create(any());
  }

  @Test
  void rejectsLaterBatchChildBeforeAnyWrite() {
    JobStore store = capableShapeStore();
    DefaultJobCreationService service = service(store, name -> true);
    DefaultBatchBuilder builder = new DefaultBatchBuilder("batch", service);
    builder.forEachInvocation(
        List.of(PlainJob.class, AnnotatedJob.class),
        DefaultJobCreationServiceTransactionalTest::invocation);
    assertThrows(UnsupportedOperationException.class, () -> service.submit(builder));
    verifyNoWrites(store);
  }

  @Test
  void rejectsBatchBranchBeforeAnyWrite() {
    JobStore store = capableShapeStore();
    DefaultJobCreationService service = service(store, name -> true);
    DefaultBatchBuilder builder = new DefaultBatchBuilder("batch", service);
    builder.forEachInvocation(
        List.of(PlainJob.class), DefaultJobCreationServiceTransactionalTest::invocation);
    builder.thenOnBatchSuccess(new InvocationAdapter(invocation(AnnotatedJob.class)));
    assertThrows(UnsupportedOperationException.class, () -> service.submit(builder));
    verifyNoWrites(store);
  }

  @Test
  void rejectsChainAndJobBranchBeforeAnyWrite() {
    for (boolean chain : List.of(true, false)) {
      JobStore store = capableShapeStore();
      DefaultJobCreationService service = service(store, name -> true);
      DefaultJobBuilder builder =
          new DefaultJobBuilder(
              service, new InvocationAdapter(invocation(PlainJob.class)), Duration.ZERO);
      if (chain) builder.then(new InvocationAdapter(invocation(AnnotatedJob.class)));
      else builder.thenOnSuccess(new InvocationAdapter(invocation(AnnotatedJob.class)));
      assertThrows(UnsupportedOperationException.class, () -> service.submit(builder));
      verifyNoWrites(store);
    }
  }

  @Test
  void rejectsStreamingProcessingActionBeforeAnyWrite() {
    JobStore store = capableShapeStore();
    DefaultJobCreationService service = service(store, name -> true);
    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("stream", service);
    builder.fromStream(Stream.of("first", "second"));
    builder.process(AnnotatedJob::consume);
    assertThrows(UnsupportedOperationException.class, () -> service.submit(builder));
    verifyNoWrites(store);
  }

  @Test
  void capableBatchKeepsParentBatchChildrenWriteOrder() {
    JobStore store = capableStore();
    preparePersistence(store);
    DefaultJobCreationService service = service(store, name -> true);
    DefaultBatchBuilder builder = new DefaultBatchBuilder("batch", service);
    builder.forEachInvocation(
        List.of(PlainJob.class, PlainJob.class),
        DefaultJobCreationServiceTransactionalTest::invocation);
    service.submit(builder);
    InOrder order = inOrder(store);
    order.verify(store).create(any());
    order.verify((BatchStore) store).saveBatch(any());
    order.verify(store).bulkInsert(any());
  }

  private static JobStore capableShapeStore() {
    return mock(
        JobStore.class,
        withSettings().extraInterfaces(BatchStore.class, WorkflowConditionStore.class));
  }

  private static void verifyNoWrites(JobStore store) {
    verify(store, never()).create(any());
    verify(store, never()).save(any());
    verify(store, never()).bulkInsert(any());
    verify((BatchStore) store, never()).saveBatch(any());
    verify((WorkflowConditionStore) store, never()).saveCondition(any());
  }

  private static JobInvocation invocation(Class<?> target) {
    return new JobInvocation(target.getName(), "run", "()V", true, List.of(), null);
  }

  static Stream<Arguments> escapeAnnotations() {
    return Stream.of(
            "jakarta.transaction.Transactional",
            "org.springframework.transaction.annotation.Transactional",
            "jakarta.ejb.TransactionAttribute",
            "io.micronaut.transaction.annotation.Transactional")
        .flatMap(
            name ->
                Stream.of("REQUIRES_NEW", "NOT_SUPPORTED", "NEVER")
                    .flatMap(
                        propagation ->
                            Stream.of(false, true)
                                .map(onClass -> Arguments.of(name, propagation, onClass))));
  }

  @ParameterizedTest
  @MethodSource("escapeAnnotations")
  void rejectsEveryEscapeAnnotationOnMethodAndClass(
      String name, String propagation, boolean onClass) {
    JobStore store = capableStore();
    DefiningLoader loader = new DefiningLoader(getClass().getClassLoader());
    Class<?> target = generatedTarget(loader, name, propagation, onClass);
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      assertThrows(
          IllegalArgumentException.class, () -> submit(service(store, allowed -> true), target));
      verify(store, never()).create(any());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  static Stream<String> transactionAnnotations() {
    return Stream.of(
        "jakarta.transaction.Transactional",
        "org.springframework.transaction.annotation.Transactional",
        "jakarta.ejb.TransactionAttribute",
        "io.micronaut.transaction.annotation.Transactional");
  }

  @ParameterizedTest
  @MethodSource("transactionAnnotations")
  void requiredPropagationIsAccepted(String name) {
    JobStore store = capableStore();
    preparePersistence(store);
    DefiningLoader loader = new DefiningLoader(getClass().getClassLoader());
    Class<?> target = generatedTarget(loader, name, "REQUIRED", false);
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      assertDoesNotThrow(() -> submit(service(store, allowed -> true), target));
      verify(store).create(any());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  private static JobStore capableStore() {
    JobStore store = capableShapeStore();
    when(store.capability(TransactionalCompletionStore.class))
        .thenReturn(Optional.of(mock(TransactionalCompletionStore.class)));
    return store;
  }

  private static void preparePersistence(JobStore store) {
    when(store.create(any()))
        .thenAnswer(
            invocation -> {
              JobEntity job = invocation.getArgument(0);
              job.setId(UUID.randomUUID());
              return job;
            });
  }

  private static DefaultJobCreationService service(JobStore store, ClassPolicy policy) {
    return new DefaultJobCreationService(
        mock(BatchService.class),
        store,
        store,
        (BatchStore) store,
        store,
        (WorkflowConditionStore) store,
        null,
        mock(JobWakeupService.class),
        null,
        new DefaultJobInvocationResolver(),
        new JobPayloadInputValidator(),
        null,
        null,
        null,
        policy,
        null,
        null,
        Clock.systemUTC(),
        false,
        false,
        null,
        mock(AfterCommitRegistrar.class),
        new DefaultErrorSanitizer());
  }

  private static void submit(DefaultJobCreationService service, Class<?> target) {
    InvocationAdapter task =
        new InvocationAdapter(
            new JobInvocation(target.getName(), "run", "()V", true, List.of(), null));
    service.submit(new DefaultJobBuilder(service, task, Duration.ZERO));
  }

  @TransactionalJob
  public static class AnnotatedJob {
    public static void run() {}

    public static void consume(String item) {}
  }

  public static class PlainJob {
    public static void run() {}
  }

  // Name-only fixtures exercise reflection without dependencies on framework annotation libraries.
  private static Class<?> generatedTarget(
      DefiningLoader loader, String annotationName, String propagation, boolean onClass) {
    String annotationType = annotationName.replace('.', '/');
    String attribute = annotationName.startsWith("org.springframework") ? "propagation" : "value";
    ClassWriter annotation = new ClassWriter(0);
    annotation.visit(
        Opcodes.V17,
        Opcodes.ACC_PUBLIC | Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT | Opcodes.ACC_ANNOTATION,
        annotationType,
        null,
        "java/lang/Object",
        new String[] {"java/lang/annotation/Annotation"});
    AnnotationVisitor retention =
        annotation.visitAnnotation(Type.getDescriptor(Retention.class), true);
    retention.visitEnum("value", Type.getDescriptor(RetentionPolicy.class), "RUNTIME");
    retention.visitEnd();
    annotation
        .visitMethod(
            Opcodes.ACC_PUBLIC | Opcodes.ACC_ABSTRACT,
            attribute,
            "()" + Type.getDescriptor(TxType.class),
            null,
            null)
        .visitEnd();
    annotation.visitEnd();
    loader.define(annotationName, annotation.toByteArray());

    String name = "run.ratchet.test.GeneratedTransactionalJob";
    ClassWriter target = new ClassWriter(0);
    target.visit(
        Opcodes.V17, Opcodes.ACC_PUBLIC, name.replace('.', '/'), null, "java/lang/Object", null);
    target.visitAnnotation(Type.getDescriptor(TransactionalJob.class), true).visitEnd();
    MethodVisitor method =
        target.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "run", "()V", null, null);
    AnnotationVisitor escape =
        onClass
            ? target.visitAnnotation("L" + annotationType + ";", true)
            : method.visitAnnotation("L" + annotationType + ";", true);
    escape.visitEnum(attribute, Type.getDescriptor(TxType.class), propagation);
    escape.visitEnd();
    method.visitCode();
    method.visitInsn(Opcodes.RETURN);
    method.visitMaxs(0, 0);
    method.visitEnd();
    target.visitEnd();
    return loader.define(name, target.toByteArray());
  }

  private static final class DefiningLoader extends ClassLoader {
    private DefiningLoader(ClassLoader parent) {
      super(parent);
    }

    private Class<?> define(String name, byte[] bytes) {
      return defineClass(name, bytes, 0, bytes.length);
    }
  }
}
