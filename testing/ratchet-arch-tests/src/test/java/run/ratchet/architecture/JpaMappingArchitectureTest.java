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
package run.ratchet.architecture;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;

class JpaMappingArchitectureTest {
  @Test
  void sharedStoresShipOnlyStandardJpaMappingElements() throws Exception {
    var root = ReactorStoreModules.findReactorRoot();
    var parser = DocumentBuilderFactory.newInstance();
    parser.setNamespaceAware(true);
    parser.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    int mappings = 0;
    for (var module : ReactorStoreModules.discover().stores()) {
      var resources = root.resolve(module.modulePath()).resolve("src/main/resources");
      if (!Files.isDirectory(resources)) continue;
      try (var paths = Files.walk(resources)) {
        for (var path :
            paths.filter(p -> p.getFileName().toString().matches("orm.*\\.xml")).toList()) {
          var document = parser.newDocumentBuilder().parse(path.toFile());
          assertEquals(
              "entity-mappings", document.getDocumentElement().getLocalName(), path.toString());
          var elements = document.getElementsByTagName("*");
          for (int i = 0; i < elements.getLength(); i++) {
            assertEquals(
                "https://jakarta.ee/xml/ns/persistence/orm",
                elements.item(i).getNamespaceURI(),
                path.toString());
          }
          mappings++;
        }
      }
    }
    assertTrue(mappings >= 5, "The guard must inspect the core and SQL mappings");
  }
}
