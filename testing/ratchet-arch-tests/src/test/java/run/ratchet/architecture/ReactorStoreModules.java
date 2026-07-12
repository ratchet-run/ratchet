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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * Discovers production store modules from the root reactor instead of maintaining a vendor list.
 */
final class ReactorStoreModules {

  private static final String ARCH_TEST_MODULE = "testing/ratchet-arch-tests";
  private static final String STORE_MODULE_PREFIX = "stores/ratchet-store-";
  private static final String STORE_PACKAGE_PREFIX = "run.ratchet.store.";

  private ReactorStoreModules() {}

  static ReactorModel discover() {
    try {
      Path reactorRoot = findReactorRoot();
      Document rootPom = parsePom(reactorRoot.resolve("pom.xml"));
      List<StoreModule> stores = new ArrayList<>();
      for (String modulePath :
          childTexts(child(rootPom.getDocumentElement(), "modules"), "module")) {
        if (!modulePath.startsWith(STORE_MODULE_PREFIX)) {
          continue;
        }
        Path moduleDirectory = reactorRoot.resolve(modulePath);
        Document modulePom = parsePom(moduleDirectory.resolve("pom.xml"));
        String artifactId = childText(modulePom.getDocumentElement(), "artifactId");
        stores.add(
            new StoreModule(
                artifactId,
                modulePath,
                productionPackages(moduleDirectory.resolve("src/main/java"))));
      }

      Document archTestPom = parsePom(reactorRoot.resolve(ARCH_TEST_MODULE).resolve("pom.xml"));
      Set<String> archTestDependencies = new LinkedHashSet<>();
      Element dependencies = child(archTestPom.getDocumentElement(), "dependencies");
      for (Element dependency : children(dependencies, "dependency")) {
        if ("run.ratchet".equals(childText(dependency, "groupId"))
            && "test".equals(childText(dependency, "scope"))) {
          archTestDependencies.add(childText(dependency, "artifactId"));
        }
      }
      return new ReactorModel(List.copyOf(stores), Set.copyOf(archTestDependencies));
    } catch (IOException | ParserConfigurationException | SAXException e) {
      throw new IllegalStateException("Cannot discover Ratchet store modules from the reactor", e);
    }
  }

  private static Path findReactorRoot()
      throws IOException, ParserConfigurationException, SAXException {
    LinkedHashSet<Path> starts = new LinkedHashSet<>();
    starts.add(Path.of("").toAbsolutePath().normalize());
    String multiModuleDirectory = System.getProperty("maven.multiModuleProjectDirectory");
    if (multiModuleDirectory != null && !multiModuleDirectory.isBlank()) {
      starts.add(Path.of(multiModuleDirectory).toAbsolutePath().normalize());
    }

    for (Path start : starts) {
      for (Path candidate = start; candidate != null; candidate = candidate.getParent()) {
        Path pom = candidate.resolve("pom.xml");
        if (!Files.isRegularFile(pom)) {
          continue;
        }
        Element modules = child(parsePom(pom).getDocumentElement(), "modules");
        if (childTexts(modules, "module").contains(ARCH_TEST_MODULE)) {
          return candidate;
        }
      }
    }
    throw new IllegalStateException("Cannot locate the Ratchet reactor root from " + starts);
  }

  private static Set<String> productionPackages(Path sourceRoot) throws IOException {
    if (!Files.isDirectory(sourceRoot)) {
      return Set.of();
    }
    LinkedHashSet<String> packages = new LinkedHashSet<>();
    try (Stream<Path> sources = Files.walk(sourceRoot)) {
      sources
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".java"))
          .filter(path -> !path.getFileName().toString().equals("module-info.java"))
          .map(Path::getParent)
          .map(sourceRoot::relativize)
          .map(path -> path.toString().replace(File.separatorChar, '.'))
          .filter(packageName -> !packageName.isBlank())
          .forEach(packages::add);
    }
    return Set.copyOf(packages);
  }

  private static Document parsePom(Path pom)
      throws ParserConfigurationException, IOException, SAXException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    factory.setXIncludeAware(false);
    factory.setExpandEntityReferences(false);
    return factory.newDocumentBuilder().parse(pom.toFile());
  }

  private static Element child(Element parent, String name) {
    if (parent == null) {
      return null;
    }
    for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node instanceof Element element && element.getTagName().equals(name)) {
        return element;
      }
    }
    return null;
  }

  private static List<Element> children(Element parent, String name) {
    if (parent == null) {
      return List.of();
    }
    List<Element> result = new ArrayList<>();
    for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node instanceof Element element && element.getTagName().equals(name)) {
        result.add(element);
      }
    }
    return result;
  }

  private static String childText(Element parent, String name) {
    Element child = child(parent, name);
    return child == null ? "" : child.getTextContent().trim();
  }

  private static List<String> childTexts(Element parent, String name) {
    return children(parent, name).stream().map(Element::getTextContent).map(String::trim).toList();
  }

  record ReactorModel(List<StoreModule> stores, Set<String> archTestDependencies) {

    List<StoreModule> implementations() {
      return stores.stream().filter(StoreModule::isImplementation).toList();
    }
  }

  record StoreModule(String artifactId, String modulePath, Set<String> productionPackages) {

    boolean isImplementation() {
      return !artifactId.equals("ratchet-store-core");
    }

    Set<String> implementationPackageRoots() {
      LinkedHashSet<String> roots = new LinkedHashSet<>();
      productionPackages.stream()
          .filter(packageName -> packageName.startsWith(STORE_PACKAGE_PREFIX))
          .map(packageName -> packageName.substring(STORE_PACKAGE_PREFIX.length()))
          .filter(suffix -> !suffix.isBlank())
          .map(suffix -> suffix.split("\\.", 2)[0])
          .map(segment -> STORE_PACKAGE_PREFIX + segment)
          .forEach(roots::add);
      if (isImplementation() && roots.isEmpty()) {
        throw new IllegalStateException(
            artifactId + " has no production package below " + STORE_PACKAGE_PREFIX);
      }
      return Set.copyOf(roots);
    }
  }
}
