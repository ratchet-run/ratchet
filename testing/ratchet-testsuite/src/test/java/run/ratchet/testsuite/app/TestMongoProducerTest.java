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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import jakarta.enterprise.inject.Disposes;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class TestMongoProducerTest {

  @Test
  void producedClient_hasDisposerMethod() {
    assertTrue(
        Arrays.stream(TestMongoProducer.class.getDeclaredMethods())
            .flatMap(method -> Arrays.stream(method.getParameters()))
            .anyMatch(
                parameter ->
                    parameter.getType() == MongoClient.class
                        && parameter.isAnnotationPresent(Disposes.class)));
  }
}
