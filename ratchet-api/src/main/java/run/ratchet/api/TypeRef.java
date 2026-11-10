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
package run.ratchet.api;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/** Captures a complete result type, for example {@code new TypeRef<Set<String>>() {}}. */
@Incubating
public abstract class TypeRef<T> {
  private final Type type;

  protected TypeRef() {
    Type parent = getClass().getGenericSuperclass();
    if (!(parent instanceof ParameterizedType parameterized)
        || parameterized.getRawType() != TypeRef.class) {
      throw new IllegalStateException("TypeRef must declare its type argument");
    }
    type = parameterized.getActualTypeArguments()[0];
  }

  @Incubating
  public final Type getType() {
    return type;
  }
}
