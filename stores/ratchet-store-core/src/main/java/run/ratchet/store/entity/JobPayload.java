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
package run.ratchet.store.entity;

import jakarta.json.bind.annotation.JsonbCreator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.objectweb.asm.Type;

/**
 * Serializable payload describing a method invocation for job execution.
 *
 * @param target target class name
 * @param method target method name
 * @param methodDescriptor JVM method descriptor
 * @param isStatic whether the target method is static
 * @param args stored invocation arguments
 * @param runtimeArgIndexes indexes of functional-interface parameters, aligned with args; null
 *     entries identify stored values. Runtime slots contain null in args. Runtime values are never
 *     persisted. This field is omitted from JSON when null.
 */
public record JobPayload(
    String target,
    String method,
    String methodDescriptor,
    boolean isStatic,
    List<Object> args,
    List<Integer> runtimeArgIndexes)
    implements Serializable {

  @JsonbCreator
  public JobPayload {
    // Runtime slots hold null, so the copy must allow null elements (List.copyOf does not).
    args = args == null ? null : Collections.unmodifiableList(new ArrayList<>(args));
    runtimeArgIndexes =
        runtimeArgIndexes == null || runtimeArgIndexes.stream().allMatch(index -> index == null)
            ? null
            : Collections.unmodifiableList(new ArrayList<>(runtimeArgIndexes));
  }

  private static final Map<Character, Class<?>> PRIMITIVE_TYPES =
      Map.of(
          'Z', boolean.class,
          'B', byte.class,
          'C', char.class,
          'S', short.class,
          'I', int.class,
          'J', long.class,
          'F', float.class,
          'D', double.class);

  private static Class<?> resolveType(Type type) throws ClassNotFoundException {
    return switch (type.getSort()) {
      case Type.VOID -> void.class;
      case Type.BOOLEAN,
          Type.BYTE,
          Type.CHAR,
          Type.SHORT,
          Type.INT,
          Type.LONG,
          Type.FLOAT,
          Type.DOUBLE ->
          PRIMITIVE_TYPES.get(type.getDescriptor().charAt(0));
      case Type.ARRAY ->
          Class.forName(
              type.getDescriptor().replace('/', '.'),
              false,
              Thread.currentThread().getContextClassLoader());
      case Type.OBJECT ->
          Class.forName(type.getClassName(), false, Thread.currentThread().getContextClassLoader());
      default -> throw new IllegalArgumentException("Unsupported ASM type sort: " + type.getSort());
    };
  }

  /**
   * Extracts the parameter types from the method descriptor.
   *
   * @throws IllegalStateException if parameter types cannot be resolved
   */
  public Class<?>[] parameterTypes() {
    Type[] asmTypes = Type.getArgumentTypes(methodDescriptor);
    Class<?>[] clz = new Class<?>[asmTypes.length];
    try {
      for (int i = 0; i < asmTypes.length; i++) {
        clz[i] = resolveType(asmTypes[i]);
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "Cannot resolve parameter types from descriptor '" + methodDescriptor + "'", e);
    }
    return clz;
  }
}
