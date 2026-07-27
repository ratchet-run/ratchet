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
  void classPolicyRejectsDisallowedJobTargetInNativeMongoApp() {
    given().when().post("/jobs/reject-class-policy").then().statusCode(200).body(is("rejected"));
  }
}
