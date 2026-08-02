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
import io.quarkus.test.junit.QuarkusTest;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Proves the ratchet-quarkus extension works end to end against a real Quarkus boot backed by a
 * self-managed profile-selected database container (standalone Testcontainers, see {@link
 * RatchetDatabaseTestResource}): a method-reference job submitted at runtime executes, a
 * {@code @Recurring} job registered via the deferred-start path (RatchetRuntimeStart ->
 * RecurringJobProcessor.onRuntimeStart) actually fires, and Ratchet's named persistence unit
 * coexists with the application's own default unit on the same datasource.
 */
@QuarkusTest
@QuarkusTestResource(RatchetDatabaseTestResource.class)
class RatchetQuarkusSmokeTest {

  @Test
  void methodReferenceJobExecutesOnQuarkus() {
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
  void capturedStringArgumentJobExecutesOnQuarkus() {
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

  /**
   * Exercises the JSON-B rehydration path in ArgumentMaterializer: the payload round-trip
   * re-serializes the captured value and reads it back as the declared parameter type.
   */
  @Test
  void capturedRecordArgumentJobExecutesOnQuarkus() {
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
   * Submitted from a class with no {@code JobSchedulerService} field or parameter, so only
   * {@code @RegisterJobSubmitter} makes it work in native. In JVM mode the annotation is inert;
   * this run proves the wiring, and the native ITs prove the annotation itself.
   */
  @Test
  void unmanagedSubmitterJobExecutesOnQuarkus() {
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

  /**
   * Registration only happens via {@code RecurringJobProcessor.onRuntimeStart()}, which only fires
   * because the extension's build step defers @Initialized(ApplicationScoped) auto-start and fires
   * RatchetRuntimeStart from a Quarkus StartupEvent instead. The job actually executing (not just
   * registering) proves that whole chain works on a live boot, not just in a unit test.
   */
  @Test
  void recurringJobRegisteredViaOnRuntimeStartExecutesOnQuarkus() {
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

  /**
   * Ratchet runs on its own {@code "ratchet"} persistence unit while the application keeps its own
   * default unit. Persisting an app entity proves the two coexist: the default unit is functional
   * (and used drop-and-create) without disturbing Ratchet's tables, which the jobs above rely on.
   */
  @Test
  void applicationDefaultPersistenceUnitCoexistsWithRatchet() {
    Assumptions.assumeFalse(
        "mongodb".equals(System.getProperty("quarkus.datasource.db-kind")),
        "MongoDB flavor intentionally runs without Hibernate ORM or an application persistence"
            + " unit.");
    given().when().post("/jobs/notes").then().statusCode(200).body(is("1"));
    given()
        .when()
        .get("/jobs/default-unit-has-ratchet-entities")
        .then()
        .statusCode(200)
        .body(is("false"));
  }
}
