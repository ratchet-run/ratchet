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
package run.ratchet.quarkus.mongodb.runtime;

import com.mongodb.MongoClientSettings;
import io.quarkus.mongodb.runtime.MongoClientCustomizer;
import jakarta.enterprise.context.ApplicationScoped;
import org.bson.UuidRepresentation;

/** Forces MongoDB UUID encoding to RFC 4122 subtype 4 for Ratchet's UUIDv7 identifiers. */
@ApplicationScoped
public class QuarkusRatchetMongoClientCustomizer implements MongoClientCustomizer {

  @Override
  public MongoClientSettings.Builder customize(MongoClientSettings.Builder builder) {
    return builder.uuidRepresentation(UuidRepresentation.STANDARD);
  }
}
