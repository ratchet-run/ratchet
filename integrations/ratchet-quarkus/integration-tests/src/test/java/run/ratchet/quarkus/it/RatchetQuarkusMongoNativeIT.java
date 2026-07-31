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

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Native-image smoke subset for the MongoDB Quarkus flavor. */
@QuarkusIntegrationTest
@QuarkusTestResource(RatchetDatabaseTestResource.class)
class RatchetQuarkusMongoNativeIT {

  @Test
  void methodReferenceJobExecutesInNativeMongoApp() {
    given().when().post("/jobs/submit").then().statusCode(200).body(is("submitted"));

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(250))
        .until(
            () ->
                given()
                    .when()
                    .get("/jobs/executed")
                    .then()
                    .extract()
                    .body()
                    .asString()
                    .equals("true"));
  }

  @Test
  void capturedStringArgumentJobExecutesInNativeMongoApp() {
    given().when().post("/jobs/submit-value").then().statusCode(200).body(is("submitted"));

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(250))
        .until(
            () ->
                given()
                    .when()
                    .get("/jobs/executed-value")
                    .then()
                    .extract()
                    .body()
                    .asString()
                    .equals("hello-native"));
  }

  @Test
  void capturedRecordArgumentJobExecutesInNativeMongoApp() {
    given().when().post("/jobs/submit-arg").then().statusCode(200).body(is("submitted"));

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(250))
        .until(
            () ->
                given()
                    .when()
                    .get("/jobs/executed-arg")
                    .then()
                    .extract()
                    .body()
                    .asString()
                    .equals("demo:7"));
  }

  /**
   * Proves {@code @RegisterJobSubmitter} substitutes for the auto-detection heuristic: {@link
   * UnmanagedSubmitter} declares no {@code JobSchedulerService} field or parameter, so without the
   * annotation its bytecode would be absent from the image and submission would fail here.
   */
  @Test
  void unmanagedSubmitterJobExecutesInNativeMongoApp() {
    given().when().post("/jobs/submit-unmanaged").then().statusCode(200).body(is("submitted"));

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(250))
        .until(
            () ->
                given()
                    .when()
                    .get("/jobs/executed-unmanaged")
                    .then()
                    .extract()
                    .body()
                    .asString()
                    .equals("unmanaged-native"));
  }

  /** Guards the native inline-lambda regression in recurring registration. */
  @Test
  void recurringJobRegisteredViaOnRuntimeStartExecutesInNativeMongoApp() {
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(250))
        .until(
            () ->
                given()
                    .when()
                    .get("/jobs/recurring-executed")
                    .then()
                    .extract()
                    .body()
                    .asString()
                    .equals("true"));
  }

  @Test
  void classPolicyRejectsDisallowedJobTargetInNativeMongoApp() {
    given().when().post("/jobs/reject-class-policy").then().statusCode(200).body(is("rejected"));
  }
}
