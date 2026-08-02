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
package run.ratchet.spring.boot.autoconfigure.mongodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import run.ratchet.store.mongodb.MongoClientFactory;
import run.ratchet.store.spi.JobStore;

class RatchetMongoAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(RatchetMongoAutoConfiguration.class));

  @Test
  void backsOffWhenConnectionStringIsUnset() {
    contextRunner.run(
        context -> {
          assertFalse(context.containsBean("ratchetMongoClient"));
          assertFalse(context.containsBean("ratchetMongoDatabase"));
          assertFalse(context.containsBean("ratchetMongoJobStore"));
          assertNoReservedSpringMongoBeanNames(context);
        });
  }

  @Test
  void userSuppliedMongoClientWinsAndDatabaseUsesRatchetBeanName() {
    MongoClient client = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    JobStore jobStore = mock(JobStore.class);
    when(client.getDatabase("ratchet_jobs")).thenReturn(database);

    contextRunner
        .withBean("customMongoClient", MongoClient.class, () -> client)
        .withBean("customJobStore", JobStore.class, () -> jobStore)
        .withPropertyValues(
            "ratchet.mongodb.connection-string=mongodb://localhost:27017",
            "ratchet.mongodb.database=ratchet_jobs")
        .run(
            context -> {
              assertFalse(context.containsBean("ratchetMongoClient"));
              assertSame(client, context.getBean(MongoClient.class));
              assertSame(database, context.getBean("ratchetMongoDatabase"));
              assertNoReservedSpringMongoBeanNames(context);
              verify(client).getDatabase("ratchet_jobs");
            });
  }

  @Test
  void beanMethodNamesUseTheRatchetMongoPrefix() throws Exception {
    Class<?> configuration = RatchetMongoAutoConfiguration.MongoStoreConfiguration.class;
    assertEquals(
        "ratchetMongoClient",
        configuration
            .getDeclaredMethod("ratchetMongoClient", RatchetMongoProperties.class)
            .getName());
    assertEquals(
        "ratchetMongoDatabase",
        configuration
            .getDeclaredMethod(
                "ratchetMongoDatabase", MongoClient.class, RatchetMongoProperties.class)
            .getName());
    assertEquals(
        "ratchetMongoJobStore",
        configuration
            .getDeclaredMethod(
                "ratchetMongoJobStore",
                MongoClient.class,
                MongoDatabase.class,
                run.ratchet.api.RatchetOptions.class,
                run.ratchet.spi.MetricsCollector.class)
            .getName());
  }

  @Test
  void outerAutoConfigurationSignaturesDoNotLinkToOptionalMongoStoreTypes() {
    boolean storeTypeInOuterMethodSignature =
        Stream.of(RatchetMongoAutoConfiguration.class.getDeclaredMethods())
            .flatMap(
                method ->
                    Stream.concat(
                        Stream.of(method.getReturnType()), Stream.of(method.getParameterTypes())))
            .map(Class::getName)
            .anyMatch(typeName -> typeName.startsWith("run.ratchet.store.mongodb."));

    assertFalse(storeTypeInOuterMethodSignature);
  }

  @Test
  void runsBeforeBootMongoClientAutoConfiguration() {
    AutoConfiguration annotation =
        RatchetMongoAutoConfiguration.class.getAnnotation(AutoConfiguration.class);

    assertNotNull(annotation);
    assertEquals(
        List.of(
            "org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration",
            "org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration"),
        List.of(annotation.beforeName()));
  }

  @Test
  void ratchetClientWinsBeforeBootsDefaultMongoClient() {
    MongoClient client = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    JobStore jobStore = mock(JobStore.class);
    when(client.getDatabase("ratchet_jobs")).thenReturn(database);

    try (MockedStatic<MongoClientFactory> factory = mockStatic(MongoClientFactory.class)) {
      factory.when(() -> MongoClientFactory.create("mongodb://localhost:27017")).thenReturn(client);

      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration.class,
                  RatchetMongoAutoConfiguration.class))
          .withBean("customJobStore", JobStore.class, () -> jobStore)
          .withPropertyValues(
              "ratchet.mongodb.connection-string=mongodb://localhost:27017",
              "ratchet.mongodb.database=ratchet_jobs")
          .run(
              context -> {
                assertArrayEquals(
                    new String[] {"ratchetMongoClient"},
                    context.getBeanNamesForType(MongoClient.class));
                assertSame(database, context.getBean("ratchetMongoDatabase"));
                assertNoReservedSpringMongoBeanNames(context);
              });

      factory.verify(() -> MongoClientFactory.create("mongodb://localhost:27017"));
    }
  }

  @Test
  void connectionStringWithoutDatabaseFailsLoudly() {
    MongoClient client = mock(MongoClient.class);
    JobStore jobStore = mock(JobStore.class);

    contextRunner
        .withBean("customMongoClient", MongoClient.class, () -> client)
        .withBean("customJobStore", JobStore.class, () -> jobStore)
        .withPropertyValues("ratchet.mongodb.connection-string=mongodb://localhost:27017")
        .run(
            context -> {
              Throwable failure = context.getStartupFailure();
              assertNotNull(failure);
              Throwable rootCause = rootCause(failure);
              assertTrue(rootCause instanceof IllegalStateException);
              assertEquals(
                  "ratchet.mongodb.database must be set when"
                      + " ratchet.mongodb.connection-string is configured",
                  rootCause.getMessage());
            });
  }

  private static void assertNoReservedSpringMongoBeanNames(
      org.springframework.context.ApplicationContext context) {
    assertFalse(context.containsBean("mongo"));
    assertFalse(context.containsBean("mongoTemplate"));
  }

  private static Throwable rootCause(Throwable failure) {
    Throwable rootCause = failure;
    while (rootCause.getCause() != null) {
      rootCause = rootCause.getCause();
    }
    return rootCause;
  }
}
