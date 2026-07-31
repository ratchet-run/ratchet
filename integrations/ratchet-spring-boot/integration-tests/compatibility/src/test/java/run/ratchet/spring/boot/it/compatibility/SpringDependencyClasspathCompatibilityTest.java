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
package run.ratchet.spring.boot.it.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.annotation.JsonbProperty;
import jakarta.transaction.Transactional;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.jar.JarFile;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ReactiveWebApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;
import org.springframework.http.MediaType;
import org.springframework.http.codec.HttpMessageWriter;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.mock.http.MockHttpOutputMessage;
import org.springframework.mock.http.server.reactive.MockServerHttpResponse;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration;
import run.ratchet.spring.boot.autoconfigure.jpa.RatchetJpaAutoConfiguration;

class SpringDependencyClasspathCompatibilityTest {

  private static final Map<Class<?>, String> SUPPLIED_API_ARTIFACTS =
      Map.of(
          Instance.class, "jakarta.enterprise.cdi-api",
          Transactional.class, "jakarta.transaction-api",
          Interceptor.class, "jakarta.interceptor-api",
          Json.class, "jakarta.json-api",
          Jsonb.class, "jakarta.json.bind-api");

  private static final List<String> SPRING_ARTIFACT_MODULES =
      List.of(
          "ratchet-spring-boot-autoconfigure",
          "ratchet-spring-boot-autoconfigure-jpa",
          "ratchet-spring-boot-autoconfigure-mongodb",
          "ratchet-spring-boot-starter",
          "ratchet-spring-boot-starter-mongodb",
          "ratchet-spring-boot-aot-spring7");

  @Test
  void ratchetSuppliesAllFiveJakartaApisAtCompileScope() throws Exception {
    Path repository = repositoryRoot();
    Path pom =
        repository.resolve(
            "integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure/pom.xml");
    Map<String, String> scopes = dependencyScopes(pom);
    List<DependencyPath> dependencyPaths = dependencyPaths();

    for (Map.Entry<Class<?>, String> api : SUPPLIED_API_ARTIFACTS.entrySet()) {
      Path location = codeSource(api.getKey());
      assertTrue(
          location.getFileName().toString().contains(api.getValue()),
          () -> api.getKey().getName() + " resolved from unexpected artifact " + location);
      assertEquals("compile", scopes.get(api.getValue()), api.getValue() + " scope drifted");
      assertTrue(
          dependencyPaths.stream()
              .filter(path -> api.getValue().equals(path.node().artifactId()))
              .anyMatch(path -> path.ancestors().stream().anyMatch(DependencyNode::ratchetSpring)),
          api.getValue() + " is not supplied by a Ratchet Spring artifact");
    }
  }

  @Test
  void ratchetSpringArtifactsNeitherDeclareNorShadeJackson() throws Exception {
    Path springRoot = repositoryRoot().resolve("integrations/ratchet-spring-boot");
    for (String module : SPRING_ARTIFACT_MODULES) {
      String pom = Files.readString(springRoot.resolve(module).resolve("pom.xml"));
      assertFalse(
          pom.contains("<groupId>com.fasterxml.jackson") || pom.contains("<groupId>tools.jackson"),
          () -> module + " must not introduce Jackson");
    }

    assertNoShadedJackson(RatchetOptions.class);
    assertNoShadedJackson(RatchetAutoConfiguration.class);
    assertNoShadedJackson(RatchetJpaAutoConfiguration.class);

    List<DependencyPath> jacksonDependencies =
        dependencyPaths().stream().filter(path -> path.node().jackson()).toList();
    assertFalse(jacksonDependencies.isEmpty(), "The web compatibility lane must supply Jackson");
    for (DependencyPath jackson : jacksonDependencies) {
      assertFalse(
          jackson.ancestors().stream().anyMatch(DependencyNode::ratchetSpring),
          () ->
              jackson.node().groupId()
                  + ":"
                  + jackson.node().artifactId()
                  + " was introduced through a Ratchet Spring artifact");
    }
  }

  @Test
  void mvcLaneKeepsBootJacksonAsThePreferredJsonConverter() {
    ConverterObservation withoutRatchet = converterObservation("ratchet.enabled=false");
    ConverterObservation withRatchet =
        converterObservation("ratchet.allow-empty-class-policy=true");

    assertEquals(withoutRatchet.converterType(), withRatchet.converterType());
    assertBootJacksonSerialization(withoutRatchet);
    assertBootJacksonSerialization(withRatchet);
  }

  @Test
  void webFluxLaneKeepsBootJacksonAsThePreferredJsonWriter() {
    ConverterObservation withoutRatchet = reactiveConverterObservation("ratchet.enabled=false");
    ConverterObservation withRatchet =
        reactiveConverterObservation("ratchet.allow-empty-class-policy=true");

    assertEquals(withoutRatchet.converterType(), withRatchet.converterType());
    assertBootJacksonSerialization(withoutRatchet);
    assertBootJacksonSerialization(withRatchet);
  }

  private static ConverterObservation converterObservation(String... properties) {
    ConverterObservationHolder result = new ConverterObservationHolder();
    new WebApplicationContextRunner()
        .withClassLoader(new FilteredClassLoader(javax.sql.DataSource.class))
        .withUserConfiguration(MvcApplication.class)
        .withPropertyValues(properties)
        .run(
            context -> {
              assertNull(context.getStartupFailure());
              try {
                RequestMappingHandlerAdapter adapter =
                    context.getBean(RequestMappingHandlerAdapter.class);
                @SuppressWarnings("unchecked")
                HttpMessageConverter<Object> converter =
                    (HttpMessageConverter<Object>)
                        adapter.getMessageConverters().stream()
                            .filter(
                                candidate ->
                                    candidate.canWrite(
                                        MapperProbe.class, MediaType.APPLICATION_JSON))
                            .findFirst()
                            .orElseThrow(
                                () -> new AssertionError("No JSON HTTP message converter"));
                MockHttpOutputMessage output = new MockHttpOutputMessage();
                converter.write(new MapperProbe("ratchet"), MediaType.APPLICATION_JSON, output);
                result.value =
                    new ConverterObservation(
                        converter.getClass(), output.getBodyAsString(StandardCharsets.UTF_8));
              } catch (IOException exception) {
                throw new AssertionError(
                    "Unable to write through Boot's JSON converter", exception);
              }
            });
    return Objects.requireNonNull(result.value, "MVC compatibility context did not execute");
  }

  private static ConverterObservation reactiveConverterObservation(String... properties) {
    ConverterObservationHolder result = new ConverterObservationHolder();
    new ReactiveWebApplicationContextRunner()
        .withClassLoader(new FilteredClassLoader(javax.sql.DataSource.class))
        .withUserConfiguration(MvcApplication.class)
        .withPropertyValues(properties)
        .run(
            context -> {
              assertNull(context.getStartupFailure());
              ServerCodecConfigurer codecs = context.getBean(ServerCodecConfigurer.class);
              ResolvableType probeType = ResolvableType.forClass(MapperProbe.class);
              @SuppressWarnings("unchecked")
              HttpMessageWriter<Object> writer =
                  (HttpMessageWriter<Object>)
                      codecs.getWriters().stream()
                          .filter(
                              candidate ->
                                  candidate.canWrite(probeType, MediaType.APPLICATION_JSON))
                          .findFirst()
                          .orElseThrow(() -> new AssertionError("No JSON HTTP message writer"));
              MockServerHttpResponse response = new MockServerHttpResponse();
              writer
                  .write(
                      reactor.core.publisher.Mono.just(new MapperProbe("ratchet")),
                      probeType,
                      MediaType.APPLICATION_JSON,
                      response,
                      Map.of())
                  .block(Duration.ofSeconds(5));
              result.value =
                  new ConverterObservation(
                      writer.getClass(), response.getBodyAsString().block(Duration.ofSeconds(5)));
            });
    return Objects.requireNonNull(result.value, "WebFlux compatibility context did not execute");
  }

  private static void assertBootJacksonSerialization(ConverterObservation observation) {
    assertTrue(observation.body().contains("\"bootName\""), observation.body());
    assertFalse(observation.body().contains("\"jsonb_name\""), observation.body());
  }

  private static Map<String, String> dependencyScopes(Path pom) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    Element project = factory.newDocumentBuilder().parse(pom.toFile()).getDocumentElement();
    Map<String, String> scopes = new LinkedHashMap<>();
    NodeList dependencies = project.getElementsByTagName("dependency");
    for (int index = 0; index < dependencies.getLength(); index++) {
      Element dependency = (Element) dependencies.item(index);
      String artifactId = directChildText(dependency, "artifactId");
      String scope = directChildText(dependency, "scope");
      scopes.putIfAbsent(artifactId, scope);
    }
    return scopes;
  }

  private static String directChildText(Element parent, String childName) {
    NodeList children = parent.getChildNodes();
    for (int index = 0; index < children.getLength(); index++) {
      Node child = children.item(index);
      if (child instanceof Element element && childName.equals(element.getTagName())) {
        return element.getTextContent().trim();
      }
    }
    return "";
  }

  private static Path repositoryRoot() throws IOException {
    Path candidate = Path.of("").toAbsolutePath();
    while (candidate != null) {
      Path pom = candidate.resolve("pom.xml");
      if (Files.isRegularFile(pom)
          && Files.isDirectory(candidate.resolve("ratchet-api"))
          && Files.readString(pom).contains("<artifactId>ratchet-parent</artifactId>")) {
        return candidate;
      }
      candidate = candidate.getParent();
    }
    throw new IOException("Unable to locate the Ratchet repository root from " + Path.of(""));
  }

  private static Path codeSource(Class<?> type) throws URISyntaxException {
    return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
  }

  private static List<DependencyPath> dependencyPaths() throws IOException {
    Path tree =
        repositoryRoot()
            .resolve(
                "integrations/ratchet-spring-boot/integration-tests/compatibility/target/dependency-tree.json");
    try (var input = Files.newInputStream(tree);
        var reader = Json.createReader(input)) {
      DependencyNode root = dependencyNode(reader.readObject());
      java.util.ArrayList<DependencyPath> paths = new java.util.ArrayList<>();
      collectDependencyPaths(root, List.of(), paths);
      return List.copyOf(paths);
    }
  }

  private static DependencyNode dependencyNode(JsonObject object) {
    JsonArray children = object.getJsonArray("children");
    return new DependencyNode(
        object.getString("groupId"),
        object.getString("artifactId"),
        object.getString("scope", ""),
        children == null
            ? List.of()
            : children.stream().map(value -> dependencyNode(value.asJsonObject())).toList());
  }

  private static void collectDependencyPaths(
      DependencyNode node, List<DependencyNode> ancestors, List<DependencyPath> paths) {
    paths.add(new DependencyPath(node, ancestors));
    List<DependencyNode> childAncestors =
        java.util.stream.Stream.concat(ancestors.stream(), java.util.stream.Stream.of(node))
            .toList();
    node.children().forEach(child -> collectDependencyPaths(child, childAncestors, paths));
  }

  private static void assertNoShadedJackson(Class<?> anchor) throws Exception {
    Path artifact = codeSource(anchor);
    if (Files.isDirectory(artifact)) {
      assertFalse(Files.exists(artifact.resolve("com/fasterxml/jackson")));
      assertFalse(Files.exists(artifact.resolve("tools/jackson")));
      return;
    }
    try (JarFile jar = new JarFile(artifact.toFile())) {
      assertFalse(
          jar.stream()
              .anyMatch(
                  entry ->
                      entry.getName().startsWith("com/fasterxml/jackson/")
                          || entry.getName().startsWith("tools/jackson/")),
          () -> artifact + " shades Jackson classes");
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableAutoConfiguration
  static class MvcApplication {}

  static final class MapperProbe {

    private final String bootName;

    MapperProbe(String bootName) {
      this.bootName = bootName;
    }

    @JsonbProperty("jsonb_name")
    public String getBootName() {
      return bootName;
    }
  }

  private record ConverterObservation(Class<?> converterType, String body) {}

  private static final class ConverterObservationHolder {

    private ConverterObservation value;
  }

  private record DependencyNode(
      String groupId, String artifactId, String scope, List<DependencyNode> children) {

    private boolean jackson() {
      return groupId.startsWith("com.fasterxml.jackson") || groupId.startsWith("tools.jackson");
    }

    private boolean ratchetSpring() {
      return "run.ratchet".equals(groupId) && SPRING_ARTIFACT_MODULES.contains(artifactId);
    }
  }

  private record DependencyPath(DependencyNode node, List<DependencyNode> ancestors) {}
}
