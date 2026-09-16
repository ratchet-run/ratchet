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
package run.ratchet.ri.payload;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.SerializedLambda;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

class AsmLambdaBytecodeCacheTest {
  @Test
  void repeatedAnalysisReadsBytecodeOnceButUsesEachCallsCapturedArguments() {
    var loader = new BytecodeLoader("work");
    withLoader(
        loader,
        () -> {
          assertEquals("first", inspect("CacheSample", "first").arguments().get(0));
          assertEquals("second", inspect("CacheSample", "second").arguments().get(0));
        });
    assertEquals(1, loader.reads.get());
  }

  @Test
  void sameClassNameInDifferentLoadersHasIndependentBytecode() {
    withLoader(
        new BytecodeLoader("first"),
        () -> assertEquals("first", inspect("CacheSample", "x").methodName()));
    withLoader(
        new BytecodeLoader("second"),
        () -> assertEquals("second", inspect("CacheSample", "x").methodName()));
  }

  @Test
  void concurrentAnalysisKeepsCapturedArgumentsSeparate() throws Exception {
    var loader = new BytecodeLoader("work");
    var executor = Executors.newFixedThreadPool(8);
    try {
      var results = new ArrayList<java.util.concurrent.Future<?>>();
      for (int i = 0; i < 32; i++) {
        String captured = "value-" + i;
        results.add(
            executor.submit(
                () ->
                    withLoader(
                        loader,
                        () ->
                            assertEquals(
                                captured, inspect("CacheSample", captured).arguments().get(0)))));
      }
      for (var result : results) result.get();
    } finally {
      executor.shutdownNow();
    }
    assertEquals(1, loader.reads.get());
  }

  @Test
  void missingBytecodeCanBeRetried() {
    var loader = new BytecodeLoader("work");
    loader.available = false;
    withLoader(
        loader, () -> assertThrows(IllegalStateException.class, () -> inspect("CacheSample", "x")));
    loader.available = true;
    withLoader(loader, () -> assertEquals("work", inspect("CacheSample", "x").methodName()));
    assertEquals(2, loader.reads.get());
  }

  @Test
  void manyClassesDoNotGrowOneLoadersCacheWithoutBound() {
    var loader = new BytecodeLoader("work");
    withLoader(
        loader,
        () -> {
          inspect("First", "x");
          for (int i = 0; i < 1024; i++) inspect("Generated" + i, "x");
          int before = loader.reads.get();
          inspect("First", "x");
          assertEquals(before + 1, loader.reads.get(), "Old class bytes must be evicted");
        });
  }

  private static AsmLambdaAnalyzer.InvocationStep inspect(String className, String captured) {
    return AsmLambdaAnalyzer.inspect(
            new SerializedLambda(
                AsmLambdaBytecodeCacheTest.class,
                "java/lang/Runnable",
                "run",
                "()V",
                MethodHandleInfo.REF_invokeStatic,
                className,
                "lambda$work$0",
                "(Ljava/lang/String;)V",
                "()V",
                new Object[] {captured}))
        .last();
  }

  private static void withLoader(ClassLoader loader, Runnable work) {
    var thread = Thread.currentThread();
    var previous = thread.getContextClassLoader();
    try {
      thread.setContextClassLoader(loader);
      work.run();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  private static final class BytecodeLoader extends ClassLoader {
    private final AtomicInteger reads = new AtomicInteger();
    private final byte[] bytes;
    private boolean available = true;

    private BytecodeLoader(String targetMethod) {
      var writer = new ClassWriter(0);
      writer.visit(Opcodes.V17, Opcodes.ACC_PUBLIC, "CacheSample", null, "java/lang/Object", null);
      var method =
          writer.visitMethod(
              Opcodes.ACC_STATIC, "lambda$work$0", "(Ljava/lang/String;)V", null, null);
      method.visitCode();
      method.visitVarInsn(Opcodes.ALOAD, 0);
      method.visitMethodInsn(
          Opcodes.INVOKESTATIC, "Target", targetMethod, "(Ljava/lang/String;)V", false);
      method.visitInsn(Opcodes.RETURN);
      method.visitMaxs(1, 1);
      method.visitEnd();
      writer.visitEnd();
      bytes = writer.toByteArray();
    }

    @Override
    public InputStream getResourceAsStream(String name) {
      reads.incrementAndGet();
      return available ? new ByteArrayInputStream(bytes) : null;
    }
  }
}
