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
package run.ratchet.quarkus.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Runs against both native flavors and the JVM smoke application. */
abstract class NativeSubmitterContract {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "instance",
        "provider",
        "constructor",
        "inherited",
        "nested",
        "local",
        "anonymous",
        "lookup-reference"
      })
  void submissionDoesNotDependOnInjectionShape(String kind) {
    given().post("/native-submitters/" + kind).then().statusCode(200).body(is("submitted"));
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(
            () ->
                given().get("/native-submitters/" + kind).then().statusCode(200).body(is("true")));
  }
}
