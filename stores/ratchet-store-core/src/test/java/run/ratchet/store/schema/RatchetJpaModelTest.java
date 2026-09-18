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
package run.ratchet.store.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.InputStream;
import java.net.URL;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/** Guards the shared Ratchet JPA entity list against drift from the canonical mapping. */
class RatchetJpaModelTest {

  @Test
  void ratchetJpaModelMatchesCanonicalOrmXml() throws Exception {
    Set<String> ormXmlEntities = loadOrmXmlEntityClasses();
    assertFalse(
        ormXmlEntities.isEmpty(),
        "META-INF/orm.xml must declare Ratchet entities; check the store-core test classpath.");

    Set<String> modelEntities = new TreeSet<>(RatchetJpaModel.ENTITY_CLASS_NAMES);

    assertEquals(ormXmlEntities, modelEntities, () -> driftMessage(ormXmlEntities, modelEntities));
  }

  private static Set<String> loadOrmXmlEntityClasses() throws Exception {
    URL ormXml = RatchetJpaModelTest.class.getClassLoader().getResource("META-INF/orm.xml");
    assertNotNull(ormXml, "META-INF/orm.xml must be present on the store-core test classpath.");

    try (InputStream input = ormXml.openStream()) {
      Document document = secureDocumentBuilderFactory().newDocumentBuilder().parse(input);
      NodeList entities = document.getElementsByTagNameNS("*", "entity");
      Set<String> entityClasses = new TreeSet<>();
      for (int i = 0; i < entities.getLength(); i++) {
        Element entity = (Element) entities.item(i);
        String className = entity.getAttribute("class");
        if (!className.isBlank()) {
          entityClasses.add(className);
        }
      }
      return entityClasses;
    }
  }

  private static DocumentBuilderFactory secureDocumentBuilderFactory() throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setXIncludeAware(false);
    factory.setExpandEntityReferences(false);
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    return factory;
  }

  private static String driftMessage(Set<String> ormXmlEntities, Set<String> modelEntities) {
    return "Ratchet JPA entity mappings drifted between META-INF/orm.xml and "
        + "RatchetJpaModel.ENTITY_CLASS_NAMES. Missing from ENTITY_CLASS_NAMES: "
        + difference(ormXmlEntities, modelEntities)
        + "; missing from META-INF/orm.xml: "
        + difference(modelEntities, ormXmlEntities)
        + ". When adding an entity to ratchet-store-core, update "
        + "RatchetJpaModel.ENTITY_CLASS_NAMES.";
  }

  private static Set<String> difference(Set<String> expected, Set<String> actual) {
    Set<String> difference = new TreeSet<>(expected);
    difference.removeAll(actual);
    return difference;
  }
}
