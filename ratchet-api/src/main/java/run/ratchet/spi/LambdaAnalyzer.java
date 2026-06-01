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

import java.io.Serializable;
import run.ratchet.api.Incubating;

/**
 * Extracts target-method metadata from serializable lambda expressions.
 *
 * @see LambdaDescriptor
 */
@Incubating
public interface LambdaAnalyzer {

  /**
   * Analyzes a serializable lambda and returns its target-method metadata.
   *
   * @throws NullPointerException if {@code lambda} is {@code null}
   * @throws IllegalStateException if the lambda cannot be serialized or its bytecode cannot be
   *     analyzed
   */
  LambdaDescriptor analyze(Serializable lambda);
}
