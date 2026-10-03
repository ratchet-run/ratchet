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
package run.ratchet.spi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import run.ratchet.api.Incubating;

/**
 * Describes a lambda expression's target method for serialization and execution.
 *
 * @param targetClass fully qualified class name containing the target method
 * @param methodName target method name
 * @param methodDescriptor JVM method descriptor for overload resolution
 * @param isStatic whether the target method is static
 * @param capturedArgs captured lambda arguments; may be {@code null}. A non-null array is
 *     defensively copied, and an empty array is distinct from {@code null}. Runtime slots hold
 *     null; consult runtimeArgIndexes to distinguish them from captured nulls.
 * @param runtimeArgIndexes indexes aligned with capturedArgs: each is the functional-interface
 *     parameter index supplying that slot, or null for a captured or constant slot. Null or an
 *     all-null list is normalized to null; other lists are defensively copied and unmodifiable.
 */
@Incubating
public record LambdaDescriptor(
    String targetClass,
    String methodName,
    String methodDescriptor,
    boolean isStatic,
    Object[] capturedArgs,
    List<Integer> runtimeArgIndexes) {
  public LambdaDescriptor {
    capturedArgs = capturedArgs == null ? null : Arrays.copyOf(capturedArgs, capturedArgs.length);
    runtimeArgIndexes =
        runtimeArgIndexes == null || runtimeArgIndexes.stream().allMatch(Objects::isNull)
            ? null
            : Collections.unmodifiableList(new ArrayList<>(runtimeArgIndexes));
  }

  @Override
  public Object[] capturedArgs() {
    return capturedArgs == null ? null : Arrays.copyOf(capturedArgs, capturedArgs.length);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof LambdaDescriptor that)) return false;
    return isStatic() == that.isStatic()
        && Objects.equals(methodName(), that.methodName())
        && Objects.equals(targetClass(), that.targetClass())
        && Objects.deepEquals(capturedArgs(), that.capturedArgs())
        && Objects.equals(methodDescriptor(), that.methodDescriptor())
        && Objects.equals(runtimeArgIndexes(), that.runtimeArgIndexes());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        targetClass(),
        methodName(),
        methodDescriptor(),
        isStatic(),
        Arrays.deepHashCode(capturedArgs()),
        runtimeArgIndexes());
  }

  @Override
  public String toString() {
    return "LambdaDescriptor{"
        + "targetClass='"
        + targetClass
        + '\''
        + ", methodName='"
        + methodName
        + '\''
        + ", methodDescriptor='"
        + methodDescriptor
        + '\''
        + ", isStatic="
        + isStatic
        + ", capturedArgs="
        + Arrays.toString(capturedArgs)
        + ", runtimeArgIndexes="
        + runtimeArgIndexes
        + '}';
  }
}
