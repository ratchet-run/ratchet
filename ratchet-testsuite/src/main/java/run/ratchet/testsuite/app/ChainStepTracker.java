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
package run.ratchet.testsuite.app;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ChainStepTracker {

  private static final CopyOnWriteArrayList<String> EXECUTION_ORDER = new CopyOnWriteArrayList<>();

  public static void stepA() {
    EXECUTION_ORDER.add("A");
  }

  public static void stepB() {
    EXECUTION_ORDER.add("B");
  }

  public static void stepBThenFail() {
    EXECUTION_ORDER.add("B");
    throw new RuntimeException("Intentional chain step failure");
  }

  public static void stepC() {
    EXECUTION_ORDER.add("C");
  }

  public static List<String> executionOrder() {
    return List.copyOf(EXECUTION_ORDER);
  }

  public static void reset() {
    EXECUTION_ORDER.clear();
  }
}
