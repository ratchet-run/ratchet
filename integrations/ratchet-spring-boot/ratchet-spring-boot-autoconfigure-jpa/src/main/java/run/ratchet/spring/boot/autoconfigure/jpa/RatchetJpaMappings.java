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
package run.ratchet.spring.boot.autoconfigure.jpa;

import jakarta.persistence.spi.PersistenceUnitInfo;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/** Identifies the core default mapping by its origin, preserving application-owned mappings. */
final class RatchetJpaMappings {
  private static final String DEFAULT_ORM = "META-INF/orm.xml";

  private RatchetJpaMappings() {}

  static PersistenceUnitInfo withoutImplicitRatchetMapping(PersistenceUnitInfo unit) {
    return transform(unit, List.of(), null);
  }

  static PersistenceUnitInfo transform(
      PersistenceUnitInfo unit, List<String> additionalClasses, String additionalMapping) {
    String entityPath = "run/ratchet/store/entity/JobEntity.class";
    URL entity = RatchetJpaMappings.class.getClassLoader().getResource(entityPath);
    URL mapping = unit.getClassLoader().getResource(DEFAULT_ORM);
    String ratchetRoot =
        entity == null
            ? null
            : entity
                .toExternalForm()
                .substring(0, entity.toExternalForm().length() - entityPath.length());
    boolean filter =
        mapping != null
            && ratchetRoot != null
            && mapping.toExternalForm().equals(ratchetRoot + DEFAULT_ORM);
    if (!filter && additionalClasses.isEmpty() && additionalMapping == null) return unit;
    List<String> classes = new ArrayList<>(unit.getManagedClassNames());
    for (String name : additionalClasses) if (!classes.contains(name)) classes.add(name);
    List<String> mappings = new ArrayList<>(unit.getMappingFileNames());
    if (filter) mappings.removeIf(DEFAULT_ORM::equals);
    if (additionalMapping != null && !mappings.contains(additionalMapping))
      mappings.add(additionalMapping);
    URL root = unit.getPersistenceUnitRootUrl();
    List<URL> archives = unit.getJarFileUrls();
    if (filter) {
      if (sameArchive(root, ratchetRoot)) root = null;
      archives = archives.stream().filter(url -> !sameArchive(url, ratchetRoot)).toList();
    }
    return RatchetPersistenceUnitView.create(
        unit, List.copyOf(classes), List.copyOf(mappings), root, archives);
  }

  private static boolean sameArchive(URL url, String root) {
    return url != null && normalizeArchive(url.toExternalForm()).equals(normalizeArchive(root));
  }

  private static String normalizeArchive(String value) {
    if (value.startsWith("jar:")) value = value.substring(4);
    while (value.endsWith("!/") || value.endsWith("/")) {
      value = value.substring(0, value.length() - (value.endsWith("!/") ? 2 : 1));
    }
    return value;
  }
}
