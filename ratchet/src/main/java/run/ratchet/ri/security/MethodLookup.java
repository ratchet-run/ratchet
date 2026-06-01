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
package run.ratchet.ri.security;

import java.lang.reflect.Method;
import org.objectweb.asm.Type;
import run.ratchet.store.entity.JobPayload;

/** Shared helpers for locating methods by name and descriptor on a class. */
public class MethodLookup {

  private MethodLookup() {}

  /** Returns the first public method matching the payload's name and descriptor, or null. */
  public static Method findMethod(Class<?> clazz, JobPayload payload) {
    for (Method m : clazz.getMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        return m;
      }
    }
    return null;
  }

  /**
   * Returns the first declared method (any visibility) matching the payload's name and descriptor,
   * or null.
   */
  public static Method findDeclaredMethod(Class<?> clazz, JobPayload payload) {
    for (Method m : clazz.getDeclaredMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        return m;
      }
    }
    return null;
  }
}
