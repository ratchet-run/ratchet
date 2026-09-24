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
package example.ratchet.mongo;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.SpringBootVersion;

/** Boot-version-compatible ordinary MongoDB connection properties for the consumer test app. */
public final class MongoConsumerProperties {

  private MongoConsumerProperties() {}

  public static Map<String, Object> connection(String uri) {
    Map<String, Object> properties = new LinkedHashMap<>();
    if (SpringBootVersion.getVersion().startsWith("3.")) {
      properties.put("spring.data.mongodb.uri", uri);
      properties.put("spring.data.mongodb.uuid-representation", "standard");
    } else {
      properties.put("spring.mongodb.uri", uri);
      properties.put("spring.mongodb.representation.uuid", "standard");
    }
    return properties;
  }
}
