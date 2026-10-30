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
package run.ratchet.micronaut.graal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/** Pure bytecode discovery; never loads or initializes application classes. */
final class ApplicationDiscovery {
  private static final List<String> FRAMEWORK_PREFIXES =
      List.of(
          "java",
          "javax",
          "jakarta",
          "jdk",
          "sun",
          "com.sun",
          "io.micronaut",
          "org.graalvm",
          "org.hibernate",
          "org.eclipse",
          "org.jboss",
          "org.slf4j",
          "ch.qos",
          "com.zaxxer",
          "org.postgresql",
          "com.mysql",
          "oracle",
          "com.microsoft",
          "com.mongodb",
          "org.bson",
          "io.netty",
          "io.micrometer",
          "io.opentelemetry",
          "net.bytebuddy",
          "org.objectweb",
          "com.fasterxml",
          "org.yaml",
          "kotlin",
          "org.reactivestreams",
          "reactor",
          "org.junit",
          "org.testcontainers",
          "run.ratchet.api",
          "run.ratchet.spi",
          "run.ratchet.ri",
          "run.ratchet.store",
          "run.ratchet.micronaut",
          "run.ratchet.micrometer",
          "run.ratchet.security",
          "run.ratchet.encryption");

  private ApplicationDiscovery() {}

  static boolean isApplicationClass(String name) {
    for (String prefix : FRAMEWORK_PREFIXES) {
      if (name.equals(prefix) || name.startsWith(prefix + ".")) return false;
    }
    for (String generated :
        List.of("$Definition", "$Introspection", "$Exec", "$Reference", "$Intercepted")) {
      if (name.contains(generated)) return false;
    }
    return !name.equals("module-info") && !name.endsWith("package-info");
  }

  static boolean referencesRatchet(byte[] bytes) {
    // Constant-pool UTF8 names are ASCII; this avoids an ASM parse for unrelated classes.
    return new String(bytes, StandardCharsets.ISO_8859_1).contains("run/ratchet/api/");
  }

  /** Seeds plus subclasses whose submitting implementation lives in an ancestor. */
  static Set<String> roots(Map<String, byte[]> classes) {
    Set<String> roots = new LinkedHashSet<>();
    for (var entry : classes.entrySet()) {
      if (isApplicationClass(entry.getKey()) && referencesRatchet(entry.getValue()))
        roots.add(entry.getKey());
    }
    boolean changed;
    do {
      changed = false;
      for (var entry : classes.entrySet()) {
        if (!isApplicationClass(entry.getKey()) || roots.contains(entry.getKey())) continue;
        ClassReader reader = new ClassReader(entry.getValue());
        Set<String> parents = new LinkedHashSet<>();
        addName(parents, reader.getSuperName());
        for (String implemented : reader.getInterfaces()) addName(parents, implemented);
        if (parents.stream().anyMatch(roots::contains)) changed |= roots.add(entry.getKey());
      }
    } while (changed);
    return Set.copyOf(roots);
  }

  /** Pure graph expansion used by JVM tests and the image feature. */
  static Set<String> relatedClasses(String name, Discovery discovery, Set<String> available) {
    Set<String> related = new LinkedHashSet<>(discovery.hierarchy());
    related.addAll(discovery.targets());
    related.addAll(discovery.bindingTypes());
    for (String nested : available) if (nested.startsWith(name + "$")) related.add(nested);
    return Set.copyOf(related);
  }

  static Set<String> discoverClasses(Map<String, byte[]> classes) {
    var pending = new ArrayDeque<>(roots(classes));
    Set<String> result = new LinkedHashSet<>();
    while (!pending.isEmpty()) {
      String name = pending.removeFirst();
      if (!isApplicationClass(name) || !result.add(name)) continue;
      byte[] bytes = classes.get(name);
      if (bytes != null) pending.addAll(relatedClasses(name, discover(bytes), classes.keySet()));
    }
    return Set.copyOf(result);
  }

  record Discovery(
      String name,
      Set<String> hierarchy,
      Set<String> targets,
      Set<String> bindingTypes,
      boolean lambdaCapturing) {}

  static Discovery discover(byte[] bytes) {
    Set<String> hierarchy = new LinkedHashSet<>();
    Set<String> targets = new LinkedHashSet<>();
    Set<String> bindings = new LinkedHashSet<>();
    boolean[] capturing = {false};
    ClassReader reader = new ClassReader(bytes);
    String name = reader.getClassName().replace('/', '.');
    reader.accept(
        new ClassVisitor(Opcodes.ASM9) {
          @Override
          public void visit(
              int version,
              int access,
              String internalName,
              String signature,
              String superclass,
              String[] interfaces) {
            addName(hierarchy, superclass);
            for (String implemented : interfaces) addName(hierarchy, implemented);
          }

          @Override
          public MethodVisitor visitMethod(
              int access, String method, String descriptor, String signature, String[] exceptions) {
            if (method.equals("$deserializeLambda$")) capturing[0] = true;
            boolean lambda = (access & Opcodes.ACC_SYNTHETIC) != 0 && method.startsWith("lambda$");
            return new MethodVisitor(Opcodes.ASM9) {
              @Override
              public AnnotationVisitor visitAnnotation(String annotation, boolean visible) {
                if (annotation.equals("Lrun/ratchet/api/Recurring;")) {
                  targets.add(name);
                  addDescriptor(bindings, descriptor);
                }
                return null;
              }

              @Override
              public void visitInvokeDynamicInsn(
                  String method, String descriptor, Handle bootstrap, Object... arguments) {
                for (Object argument : arguments) {
                  if (argument instanceof Handle handle
                      && handle.getTag() >= Opcodes.H_INVOKEVIRTUAL) {
                    target(handle.getOwner(), handle.getDesc());
                  }
                }
              }

              @Override
              public void visitMethodInsn(
                  int opcode, String owner, String method, String descriptor, boolean isInterface) {
                if (lambda) target(owner, descriptor);
              }

              private void target(String owner, String descriptor) {
                String target = owner.replace('/', '.');
                if (isApplicationClass(target)) {
                  targets.add(target);
                  addDescriptor(bindings, descriptor);
                }
              }
            };
          }
        },
        ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    return new Discovery(
        name, Set.copyOf(hierarchy), Set.copyOf(targets), Set.copyOf(bindings), capturing[0]);
  }

  private static void addDescriptor(Set<String> names, String descriptor) {
    for (Type parameter : Type.getArgumentTypes(descriptor)) addType(names, parameter);
    addType(names, Type.getReturnType(descriptor));
  }

  private static void addType(Set<String> names, Type type) {
    if (type.getSort() == Type.ARRAY) type = type.getElementType();
    if (type.getSort() == Type.OBJECT && isApplicationClass(type.getClassName()))
      names.add(type.getClassName());
  }

  private static void addName(Set<String> names, String name) {
    if (name != null && isApplicationClass(name.replace('/', '.')))
      names.add(name.replace('/', '.'));
  }
}
