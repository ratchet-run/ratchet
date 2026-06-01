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

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * {@link Consumer} that is also {@link Serializable}.
 *
 * <p>Use this for callbacks that Ratchet may persist with a job or batch definition. Captured state
 * must itself be serializable.
 *
 * @param <T> consumed value type
 */
@FunctionalInterface
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

  /** {@inheritDoc} */
  @Override
  void accept(T t);
}
