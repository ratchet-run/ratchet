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

import java.util.ArrayList;
import java.util.List;
import run.ratchet.store.entity.JobPayload;

/** Binds functional parameters to explicitly indexed invocation slots. */
public final class RuntimeArguments {
  private RuntimeArguments() {}

  public static JobPayload bind(JobPayload payload, List<Object> runtimeValues, String what) {
    if (payload.runtimeArgIndexes() == null) {
      return payload;
    }
    if (payload.runtimeArgIndexes().size() != payload.args().size()) {
      throw new IllegalStateException(
          what
              + " has "
              + payload.runtimeArgIndexes().size()
              + " runtime argument indexes for "
              + payload.args().size()
              + " arguments");
    }
    List<Object> args = new ArrayList<>(payload.args());
    for (int i = 0; i < args.size(); i++) {
      Integer index = payload.runtimeArgIndexes().get(i);
      if (index == null) {
        continue;
      }
      if (index < 0 || index >= runtimeValues.size()) {
        throw new IllegalStateException(
            what
                + " expects runtime parameter "
                + index
                + " but only "
                + runtimeValues.size()
                + (runtimeValues.size() == 1 ? " is supplied" : " are supplied"));
      }
      args.set(i, runtimeValues.get(index));
    }
    return new JobPayload(
        payload.target(),
        payload.method(),
        payload.methodDescriptor(),
        payload.isStatic(),
        args,
        payload.runtimeArgIndexes());
  }
}
