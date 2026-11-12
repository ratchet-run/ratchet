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

import jakarta.json.bind.annotation.JsonbCreator;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobPayload;

/** Submission state for a reducing batch; validated before persistence. */
public final class BatchReducer {
  private final Type explicitType;
  private final Object identity;
  private final JobPayload combiner;
  private Type resultType;
  private JobPayload callback;
  private WorkflowBranch successBranch;
  private final List<JobPayload> childTargets = new ArrayList<>();

  void childTarget(JobPayload payload) {
    childTargets.add(payload);
  }

  void successBranch(WorkflowBranch branch) {
    successBranch = branch;
  }

  WorkflowBranch successBranch() {
    return successBranch;
  }

  BatchReducer(Type explicitType, Object identity, JobPayload combiner) {
    this.explicitType = explicitType;
    this.identity = Objects.requireNonNull(identity, "identity");
    this.combiner = Objects.requireNonNull(combiner, "combiner");
  }

  void callback(JobPayload callback) {
    if (this.callback != null)
      throw new IllegalStateException("Only one batch success callback is allowed");
    Objects.requireNonNull(callback, "callback");
    if (!List.of(0).equals(callback.runtimeArgIndexes())) {
      throw new IllegalArgumentException(
          "Batch success callback must declare exactly one result slot [0]");
    }
    this.callback = callback;
  }

  void validate(ClassPolicy policy, PayloadSerializer serializer) {
    Method method = BatchResultTypes.method(combiner, policy);
    resultType =
        BatchResultTypes.boxed(explicitType == null ? method.getGenericReturnType() : explicitType);
    BatchResultTypes.validate(resultType, policy);
    Type[] parameters = method.getGenericParameterTypes();
    if (parameters.length != 2 || !List.of(0, 1).equals(combiner.runtimeArgIndexes())) {
      throw new IllegalArgumentException("Combiner must have exactly two result parameters");
    }
    if (explicitType == null) {
      if (!resultType.equals(BatchResultTypes.boxed(parameters[0]))
          || !resultType.equals(BatchResultTypes.boxed(parameters[1]))) {
        throw new IllegalArgumentException("Combiner parameters and return type must equal R");
      }
    } else {
      for (Type parameter : parameters) {
        if (concrete(parameter) && !resultType.equals(BatchResultTypes.boxed(parameter))) {
          throw new IllegalArgumentException("Combiner concrete parameter types must equal R");
        }
      }
      Class<?> raw = BatchResultTypes.raw(resultType);
      if (!((Class<?>) BatchResultTypes.boxed(method.getParameterTypes()[0])).isAssignableFrom(raw)
          || !((Class<?>) BatchResultTypes.boxed(method.getParameterTypes()[1]))
              .isAssignableFrom(raw)
          || !raw.isAssignableFrom((Class<?>) BatchResultTypes.boxed(method.getReturnType()))) {
        throw new IllegalArgumentException("Combiner raw parameters and return type must match R");
      }
      if (concrete(method.getGenericReturnType())
          && !resultType.equals(BatchResultTypes.boxed(method.getGenericReturnType()))) {
        throw new IllegalArgumentException("Combiner generic return type must equal R");
      }
    }
    try {
      Object restored = serializer.deserialize(serializer.serialize(identity), resultType);
      if (restored == null) throw new IllegalArgumentException("Identity round trip returned null");
    } catch (RuntimeException ex) {
      throw new IllegalArgumentException("Batch identity cannot round trip as R", ex);
    }
    for (JobPayload child : childTargets) validateChild(child, policy);
    if (callback != null) {
      Method target = BatchResultTypes.method(callback, policy);
      if (target.getGenericParameterTypes().length != 1
          || !resultType.equals(BatchResultTypes.boxed(target.getGenericParameterTypes()[0]))) {
        throw new IllegalArgumentException("Batch success callback parameter must equal R exactly");
      }
    }
  }

  private static boolean concrete(Type type) {
    if (type instanceof Class<?>) return true;
    if (type instanceof ParameterizedType p) {
      for (Type argument : p.getActualTypeArguments()) if (!concrete(argument)) return false;
      return true;
    }
    return false;
  }

  void validateChild(JobPayload child, ClassPolicy policy) {
    Method method = BatchResultTypes.method(child, policy);
    Type actual = BatchResultTypes.boxed(method.getGenericReturnType());
    if (!resultType.equals(actual)
        && !(resultType instanceof Class<?> raw
            && raw.isAssignableFrom((Class<?>) BatchResultTypes.boxed(method.getReturnType())))) {
      throw new IllegalArgumentException("Batch child return type must match R");
    }
  }

  String json(PayloadSerializer serializer) {
    return serializer.serialize(new Stored(combiner, identity, resultType.getTypeName()));
  }

  public record Stored(JobPayload combiner, Object identity, String type) {
    @JsonbCreator
    public Stored(JobPayload combiner, Object identity, String type) {
      this.combiner = combiner;
      this.identity = identity;
      this.type = type;
    }
  }
}
