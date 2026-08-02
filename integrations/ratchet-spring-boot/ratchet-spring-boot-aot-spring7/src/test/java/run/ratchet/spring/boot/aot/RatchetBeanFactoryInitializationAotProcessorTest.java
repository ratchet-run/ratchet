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
package run.ratchet.spring.boot.aot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.aot.hint.MemberCategory.ACCESS_DECLARED_FIELDS;
import static org.springframework.aot.hint.MemberCategory.ACCESS_PUBLIC_FIELDS;
import static org.springframework.aot.hint.MemberCategory.DECLARED_CLASSES;
import static org.springframework.aot.hint.MemberCategory.INVOKE_DECLARED_CONSTRUCTORS;
import static org.springframework.aot.hint.MemberCategory.INVOKE_DECLARED_METHODS;
import static org.springframework.aot.hint.predicate.RuntimeHintsPredicates.reflection;
import static org.springframework.aot.hint.predicate.RuntimeHintsPredicates.resource;

import java.io.IOException;
import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.ReflectPermission;
import java.lang.reflect.WildcardType;
import java.net.URI;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aot.generate.GeneratedClasses;
import org.springframework.aot.generate.GeneratedFiles;
import org.springframework.aot.generate.GeneratedFiles.Kind;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.TypeReference;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotContribution;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.Recurring;
import run.ratchet.api.SerializableBiConsumer;
import run.ratchet.api.SerializableCheckedConsumer;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.util.JobPlaceholders;
import run.ratchet.spring.boot.autoconfigure.RegisterJobSubmitter;

class RatchetBeanFactoryInitializationAotProcessorTest {

  private static final String JPA_LOGGER_IMPLEMENTATION =
      "org.hibernate.jpa.internal.JpaLogger_$logger";
  private static final String HIBERNATE_VALIDATOR_BUNDLE_IMPLEMENTATION =
      "org.hibernate.validator.internal.util.logging.Messages_$bundle";
  private static final String HIBERNATE_COLUMN_ORDERING_STRATEGY_STANDARD =
      "org.hibernate.boot.model.relational.ColumnOrderingStrategyStandard";
  private static final String HIBERNATE_MAPPING_MODEL_PREFIX =
      "org.hibernate.boot.jaxb.mapping.spi.Jaxb";
  private static final String HIBERNATE_AUTO_FLUSH_EVENT_LISTENER_ARRAY =
      "org.hibernate.event.spi.AutoFlushEventListener[]";
  private static final String HIBERNATE_LOAD_EVENT_LISTENER_ARRAY =
      "org.hibernate.event.spi.LoadEventListener[]";
  private static final String RATCHET_STRING_IDENTIFIER_ARRAY = "java.lang.String[]";
  private static final String RATCHET_UUID_IDENTIFIER_ARRAY = "java.util.UUID[]";
  private static final int MINIMUM_HIBERNATE_EVENT_LISTENER_ARRAY_HINT_COUNT = 30;
  private static final int MINIMUM_HIBERNATE_STRATEGY_HINT_COUNT = 50;
  private static final List<String> HIBERNATE_XML_RESOURCES =
      List.of(
          "org/hibernate/hibernate-configuration-3.0.dtd",
          "org/hibernate/hibernate-configuration-4.0.xsd",
          "org/hibernate/hibernate-mapping-3.0.dtd",
          "org/hibernate/hibernate-mapping-4.0.xsd",
          "org/hibernate/jpa/orm_1_0.xsd",
          "org/hibernate/jpa/orm_2_0.xsd",
          "org/hibernate/jpa/orm_2_1.xsd",
          "org/hibernate/jpa/orm_2_2.xsd",
          "org/hibernate/jpa/orm_3_0.xsd",
          "org/hibernate/jpa/orm_3_1.xsd",
          "org/hibernate/jpa/orm_3_2.xsd",
          "org/hibernate/jpa/persistence_1_0.xsd",
          "org/hibernate/jpa/persistence_2_0.xsd",
          "org/hibernate/jpa/persistence_2_1.xsd",
          "org/hibernate/jpa/persistence_2_2.xsd",
          "org/hibernate/jpa/persistence_3_0.xsd",
          "org/hibernate/jpa/persistence_3_1.xsd",
          "org/hibernate/jpa/persistence_3_2.xsd",
          "org/hibernate/xsd/cfg/configuration-3.2.0.xsd",
          "org/hibernate/xsd/cfg/legacy-configuration-4.0.xsd",
          "org/hibernate/xsd/mapping/legacy-mapping-4.0.xsd",
          "org/hibernate/xsd/mapping/mapping-3.1.0.xsd",
          "org/hibernate/xsd/mapping/mapping-7.0.xsd");

  @Test
  void registersLambdaResourcesAndSupplementaryMetadataWithoutInstantiatingBeans()
      throws IOException {
    DefaultListableBeanFactory beanFactory = beanFactory("run.ratchet.spring.boot.aot");
    beanFactory.registerBeanDefinition("target", new RootBeanDefinition(JobTarget.class));
    beanFactory.registerBeanDefinition("submitter", new RootBeanDefinition(JobSubmitter.class));
    beanFactory.registerBeanDefinition(
        "ordinaryMethod", new RootBeanDefinition(OrdinaryMethodParameter.class));
    beanFactory.registerBeanDefinition(
        "inheritedSubmitter", new RootBeanDefinition(InheritedSubmitter.class));
    beanFactory.registerBeanDefinition("outside", new RootBeanDefinition(URI.class));
    ProcessingResult result = process(beanFactory);
    RuntimeHints hints = result.hints();

    assertTrue(
        reflection()
            .onType(JobTarget.class)
            .withMemberCategories(INVOKE_DECLARED_CONSTRUCTORS, INVOKE_DECLARED_METHODS)
            .test(hints));
    assertFalse(reflection().onType(URI.class).test(hints));

    assertLambdaHints(hints, JobSubmitter.class);
    assertLambdaHints(hints, OrdinaryMethodParameter.class);
    assertLambdaHints(hints, InjectingSubmitterBase.class);
    assertLambdaHints(hints, InheritedSubmitter.class);
    assertLambdaHints(hints, ExplicitSubmitter.class);
    assertLambdaHints(hints, AbstractExplicitSubmitter.class);
    assertEquals(0, lambdaHints(hints, JobTarget.class).size());

    assertTrue(resource().forResource(classResource(JobSubmitter.class)).test(hints));
    assertTrue(resource().forResource(classResource(OrdinaryMethodParameter.class)).test(hints));
    assertTrue(resource().forResource(classResource(ExplicitSubmitter.class)).test(hints));
    assertTrue(resource().forResource(classResource(InjectingSubmitterBase.class)).test(hints));
    assertTrue(resource().forResource(classResource(InheritedSubmitter.class)).test(hints));
    assertTrue(resource().forResource(classResource(AbstractExplicitSubmitter.class)).test(hints));
    assertTrue(
        resource()
            .forResource(RatchetBeanFactoryInitializationAotProcessor.AOT_MANIFEST_RESOURCE)
            .test(hints));

    String metadata =
        result
            .files()
            .getGeneratedFileContent(
                Kind.RESOURCE,
                RatchetBeanFactoryInitializationAotProcessor.LAMBDA_METADATA_RESOURCE);
    assertEquals(36, occurrences(metadata, "\"name\": \"writeReplace\""));
    assertEquals(36, occurrences(metadata, "\"parameterTypes\": []"));
    assertTrue(metadata.contains("\"declaringClass\": \"" + JobSubmitter.class.getName()));
    assertTrue(metadata.contains("\"declaringClass\": \"" + ExplicitSubmitter.class.getName()));
    assertTrue(
        metadata.contains("\"interfaces\": [\"" + SerializableCheckedRunnable.class.getName()));
    assertFalse(metadata.contains("\"serialization\""));
    assertFalse(metadata.contains("lambdaCapturingTypes"));

    String manifest =
        result
            .files()
            .getGeneratedFileContent(
                Kind.RESOURCE, RatchetBeanFactoryInitializationAotProcessor.AOT_MANIFEST_RESOURCE);
    assertTrue(manifest.startsWith("# ratchet-aot-manifest v1\n"));
    List<String> classNames = manifest.lines().skip(1).toList();
    List<String> sorted = new ArrayList<>(classNames);
    sorted.sort(String::compareTo);
    assertEquals(sorted, classNames);
    assertTrue(classNames.contains(JobTarget.class.getName()));
    assertTrue(classNames.contains(JobSubmitter.class.getName()));
    assertTrue(classNames.contains(ExplicitSubmitter.class.getName()));
    assertTrue(classNames.contains(AbstractExplicitSubmitter.class.getName()));
    assertTrue(classNames.contains(InjectingSubmitterBase.class.getName()));
  }

  @Test
  void registersBoundedPayloadRecurringAndRatchetInternalReflection() throws Exception {
    DefaultListableBeanFactory beanFactory = beanFactory("run.ratchet.spring.boot.aot");
    beanFactory.registerBeanDefinition(
        "payloadTarget", new RootBeanDefinition(PayloadTarget.class));
    beanFactory.registerBeanDefinition(
        "recurringTarget", new RootBeanDefinition(RecurringTarget.class));
    beanFactory.registerBeanDefinition(
        "interfaceTarget", new RootBeanDefinition(InterfaceTarget.class));

    ProcessingResult result = process(beanFactory);
    RuntimeHints hints = result.hints();

    assertTrue(payloadReflection(PayloadRequest.class, hints));
    assertTrue(payloadReflection(NestedPayload.class, hints));
    assertTrue(payloadReflection(CyclicPayload.class, hints));
    assertTrue(payloadReflection(InterfacePayload.class, hints));
    assertFalse(reflection().onType(URI.class).test(hints));
    assertTrue(reflection().onMethod(RecurringBase.class.getMethod("recurringJob")).test(hints));

    TypeReference jobPayload = TypeReference.of("run.ratchet.store.entity.JobPayload");
    assertTrue(
        reflection()
            .onType(jobPayload)
            .withMemberCategories(
                INVOKE_DECLARED_CONSTRUCTORS, INVOKE_DECLARED_METHODS, ACCESS_DECLARED_FIELDS)
            .test(hints));
    assertTrue(
        reflection()
            .onMethod(
                RecurringMethodInvoker.class.getMethod(
                    "invoke", String.class, String.class, boolean.class))
            .test(hints));
    assertTrue(reflection().onMethod(JobPlaceholders.class.getMethod("noop")).test(hints));
    assertTrue(
        reflection().onMethod(Executors.class, "newVirtualThreadPerTaskExecutor").test(hints));

    String manifest =
        result
            .files()
            .getGeneratedFileContent(
                Kind.RESOURCE, RatchetBeanFactoryInitializationAotProcessor.AOT_MANIFEST_RESOURCE);
    assertTrue(manifest.contains(PayloadRequest.class.getName() + "\n"));
    assertTrue(manifest.contains(NestedPayload.class.getName() + "\n"));
    assertTrue(manifest.contains(RecurringTarget.class.getName() + "\n"));
    assertTrue(manifest.contains(InterfacePayload.class.getName() + "\n"));
  }

  @Test
  void registersRatchetRuntimeClasspathResources() {
    RuntimeHints hints = process(beanFactory("")).hints();

    assertTrue(
        resource()
            .forResource(RatchetBeanFactoryInitializationAotProcessor.JPA_MAPPING_RESOURCE)
            .test(hints));
    assertTrue(
        resource()
            .forResource(RatchetBeanFactoryInitializationAotProcessor.STORE_CORE_ANCHOR_RESOURCE)
            .test(hints));
    assertTrue(
        resource()
            .forResource(
                RatchetBeanFactoryInitializationAotProcessor.SCHEMA_MIGRATION_INDEX_RESOURCE)
            .test(hints));
    for (String migrationResource :
        List.of(
            "ddl/migrations/V001__initial_schema.sql",
            "ddl/migrations/V002__job_extension_store.sql",
            "ddl/migrations/V003__workflow_evaluation_order.sql",
            "ddl/migrations/V004__recurring_misfire_policy.sql",
            "ddl/migrations/V005__remove_dead_dlq_alert_subsystem.sql",
            "ddl/migrations/V006__drop_recurring_master_fk.sql")) {
      assertTrue(resource().forResource(migrationResource).test(hints));
    }

    assertFalse(resource().forResource("ddl/postgresql-schema.sql").test(hints));
    assertFalse(resource().forResource("ddl/mongodb-init.js").test(hints));
  }

  @Test
  void registersPostgresqlProviderHintsWhenDriverIsPresent() throws Exception {
    RuntimeHints hints = process(beanFactory("")).hints();

    assertPostgresqlProviderHints(hints, true);
  }

  @Test
  void registersHikariPoolHintsWhenDataSourceIsPresent() throws Exception {
    RuntimeHints hints = process(beanFactory("")).hints();

    assertHikariPoolHints(hints, true);
  }

  @Test
  void registersCompleteHibernateLocalXmlSchemaInventory() {
    RuntimeHints hints = process(beanFactory("")).hints();

    assertEquals(
        List.of("org/hibernate/*.dtd", "org/hibernate/**/*.xsd"),
        RatchetBeanFactoryInitializationAotProcessor.HIBERNATE_XML_RESOURCE_PATTERNS);
    HIBERNATE_XML_RESOURCES.forEach(
        resourceName -> assertTrue(resource().forResource(resourceName).test(hints), resourceName));
    assertFalse(resource().forResource("org/hibernate/hibernate.properties").test(hints));
  }

  @Test
  void registersHibernateGeneratedLoggerAndBundleReflection() throws Exception {
    RuntimeHints hints = process(beanFactory("")).hints();

    List<String> loggerClassNames = hibernateGeneratedHintClassNames(hints, "_$logger");
    List<String> bundleClassNames = hibernateGeneratedHintClassNames(hints, "_$bundle");

    assertEquals(54, loggerClassNames.size());
    assertEquals(1, bundleClassNames.size());
    assertTrue(loggerClassNames.contains(JPA_LOGGER_IMPLEMENTATION));
    assertTrue(bundleClassNames.contains(HIBERNATE_VALIDATOR_BUNDLE_IMPLEMENTATION));

    Class<?> loggerType = Class.forName(JPA_LOGGER_IMPLEMENTATION);
    for (var constructor : loggerType.getDeclaredConstructors()) {
      assertTrue(reflection().onConstructorInvocation(constructor).test(hints));
    }

    Class<?> bundleType = Class.forName(HIBERNATE_VALIDATOR_BUNDLE_IMPLEMENTATION);
    for (var constructor : bundleType.getDeclaredConstructors()) {
      assertTrue(reflection().onConstructorInvocation(constructor).test(hints));
    }
    assertTrue(
        reflection()
            .onType(bundleType)
            .withMemberCategories(
                INVOKE_DECLARED_CONSTRUCTORS, ACCESS_DECLARED_FIELDS, ACCESS_PUBLIC_FIELDS)
            .test(hints));
    assertTrue(reflection().onFieldAccess(bundleType.getField("INSTANCE")).test(hints));

    System.out.printf(
        "Hibernate generated reflection scan: logger=%d, bundle=%d%n",
        loggerClassNames.size(), bundleClassNames.size());
  }

  @Test
  void registersHibernateDefaultStrategyConstructorReflection() throws Exception {
    RuntimeHints hints = process(beanFactory("")).hints();

    List<String> strategyClassNames = hibernateStrategyHintClassNames(hints);
    assertTrue(
        strategyClassNames.size() >= MINIMUM_HIBERNATE_STRATEGY_HINT_COUNT,
        () -> "Expected Hibernate strategy reflection hints, got " + strategyClassNames);
    assertTrue(strategyClassNames.contains(HIBERNATE_COLUMN_ORDERING_STRATEGY_STANDARD));

    Class<?> strategyType = Class.forName(HIBERNATE_COLUMN_ORDERING_STRATEGY_STANDARD);
    for (var constructor : strategyType.getDeclaredConstructors()) {
      assertTrue(reflection().onConstructorInvocation(constructor).test(hints));
    }

    System.out.printf(
        "Hibernate strategy reflection scan: strategy=%d%n", strategyClassNames.size());
  }

  @Test
  void registersCompleteHibernateEventListenerArrayFamily() {
    RuntimeHints hints = process(beanFactory("")).hints();

    List<String> listenerArrayClassNames = hibernateEventListenerArrayHintClassNames(hints);
    assertTrue(
        listenerArrayClassNames.size() >= MINIMUM_HIBERNATE_EVENT_LISTENER_ARRAY_HINT_COUNT,
        () -> "Expected Hibernate event listener array hints, got " + listenerArrayClassNames);
    assertTrue(listenerArrayClassNames.contains(HIBERNATE_AUTO_FLUSH_EVENT_LISTENER_ARRAY));
    assertTrue(listenerArrayClassNames.contains(HIBERNATE_LOAD_EVENT_LISTENER_ARRAY));

    System.out.printf(
        "Hibernate event listener array reflection scan: listener-array=%d%n",
        listenerArrayClassNames.size());
  }

  @Test
  void registersRatchetEntityIdentifierArrayFamily() {
    RuntimeHints hints = process(beanFactory("")).hints();

    assertTrue(reflection().onType(TypeReference.of(RATCHET_STRING_IDENTIFIER_ARRAY)).test(hints));
    assertTrue(reflection().onType(TypeReference.of(RATCHET_UUID_IDENTIFIER_ARRAY)).test(hints));
  }

  @Test
  void registersHibernateOrmXmlBinderReflectionObservedByTheTracingAgent() throws Exception {
    RuntimeHints hints = process(beanFactory("")).hints();

    List<String> mappingModelClassNames = hibernateMappingModelHintClassNames(hints);
    assertEquals(112, mappingModelClassNames.size());
    assertFalse(
        mappingModelClassNames.contains(
            RatchetBeanFactoryInitializationAotProcessor
                .HIBERNATE_UNTRACED_MAPPING_MODEL_CLASS_NAME));

    Class<?> mappingsType =
        Class.forName("org.hibernate.boot.jaxb.mapping.spi.JaxbEntityMappingsImpl");
    assertTrue(
        reflection().onConstructorInvocation(mappingsType.getDeclaredConstructor()).test(hints));
    for (String fieldName : List.of("converters", "entities", "version")) {
      assertTrue(reflection().onFieldAccess(mappingsType.getDeclaredField(fieldName)).test(hints));
    }

    Class<?> pluralFetchType =
        Class.forName("org.hibernate.boot.jaxb.mapping.spi.JaxbPluralFetchModeImpl");
    for (String fieldName : List.of("JOIN", "SELECT", "SUBSELECT")) {
      assertTrue(reflection().onFieldAccess(pluralFetchType.getField(fieldName)).test(hints));
    }

    assertTrue(
        reflection().onConstructorInvocation(ArrayList.class.getDeclaredConstructor()).test(hints));
    assertTrue(
        reflection().onType(Types.class).withMemberCategories(ACCESS_PUBLIC_FIELDS).test(hints));
    for (String methodName :
        List.of("getAnnotatedReceiverType", "getParameterCount", "getParameters")) {
      assertTrue(
          reflection().onMethodInvocation(Executable.class.getMethod(methodName)).test(hints),
          methodName);
    }
    for (String methodName : List.of("getModifiers", "isNamePresent")) {
      assertTrue(
          reflection().onMethodInvocation(Parameter.class.getMethod(methodName)).test(hints),
          methodName);
    }
    for (String methodName : List.of("getActualTypeArguments", "getRawType")) {
      assertTrue(
          reflection()
              .onMethodInvocation(ParameterizedType.class.getMethod(methodName))
              .test(hints),
          methodName);
    }
    for (String methodName : List.of("getLowerBounds", "getUpperBounds")) {
      assertTrue(
          reflection().onMethodInvocation(WildcardType.class.getMethod(methodName)).test(hints),
          methodName);
    }
    assertTrue(
        reflection()
            .onConstructorInvocation(ReflectPermission.class.getConstructor(String.class))
            .test(hints));

    assertTrue(
        reflection()
            .onMethodInvocation(jakarta.persistence.CollectionTable.class.getMethod("options"))
            .test(hints));
    assertTrue(
        reflection()
            .onMethodInvocation(jakarta.xml.bind.annotation.XmlElement.class.getMethod("type"))
            .test(hints));
    Class<?> navigatorType =
        Class.forName("org.glassfish.jaxb.core.v2.model.nav.ReflectionNavigator");
    assertTrue(
        reflection()
            .onMethodInvocation(navigatorType.getDeclaredMethod("getInstance"))
            .test(hints));
  }

  @Test
  void registersRemainingTraceBackedHibernateBootstrapReflection() throws Exception {
    RuntimeHints hints = process(beanFactory("")).hints();

    for (String className :
        RatchetBeanFactoryInitializationAotProcessor.HIBERNATE_ANNOTATION_CATALOG_CLASS_NAMES) {
      assertTrue(
          reflection()
              .onType(Class.forName(className))
              .withMemberCategories(ACCESS_PUBLIC_FIELDS)
              .test(hints));
    }
    for (var constructorHint :
        RatchetBeanFactoryInitializationAotProcessor.HIBERNATE_ANNOTATION_WRAPPER_CONSTRUCTORS) {
      Class<?>[] parameterTypes =
          constructorHint.parameterTypeNames().stream()
              .map(RatchetBeanFactoryInitializationAotProcessorTest::loadClass)
              .toArray(Class<?>[]::new);
      assertTrue(
          reflection()
              .onConstructorInvocation(
                  Class.forName(constructorHint.className()).getDeclaredConstructor(parameterTypes))
              .test(hints),
          constructorHint.className());
    }

    Class<?> eventType = Class.forName("org.hibernate.event.spi.EventType");
    assertTrue(
        reflection().onType(eventType).withMemberCategories(ACCESS_PUBLIC_FIELDS).test(hints));
    Class<?> sqlTypes = Class.forName("org.hibernate.type.SqlTypes");
    assertTrue(
        reflection().onType(sqlTypes).withMemberCategories(ACCESS_PUBLIC_FIELDS).test(hints));

    Class<?> collectionPersister =
        Class.forName("org.hibernate.persister.collection.BasicCollectionPersister");
    assertTrue(
        reflection()
            .onConstructorInvocation(
                collectionPersister.getDeclaredConstructor(
                    Class.forName("org.hibernate.mapping.Collection"),
                    Class.forName("org.hibernate.cache.spi.access.CollectionDataAccess"),
                    Class.forName("org.hibernate.metamodel.spi.RuntimeModelCreationContext")))
            .test(hints));
    Class<?> dataSourceProvider =
        Class.forName(
            "org.hibernate.engine.jdbc.connections.internal.DataSourceConnectionProvider");
    assertTrue(
        reflection()
            .onMethodInvocation(
                dataSourceProvider.getMethod(
                    "setJndiService", Class.forName("org.hibernate.engine.jndi.spi.JndiService")))
            .test(hints));

    for (String className :
        RatchetBeanFactoryInitializationAotProcessor.HIBERNATE_POSTGRESQL_JDBC_TYPE_CLASS_NAMES) {
      Class<?> type = Class.forName(className);
      assertTrue(
          reflection().onConstructorInvocation(type.getDeclaredConstructor()).test(hints),
          className);
    }
  }

  @Test
  void registersHibernateDialectOverrideBootstrapReflection() throws Exception {
    RuntimeHints hints = process(beanFactory("")).hints();

    Class<?> dialectOverrideType =
        Class.forName(
            RatchetBeanFactoryInitializationAotProcessor.HIBERNATE_DIALECT_OVERRIDE_CLASS_NAME);
    assertTrue(
        reflection()
            .onType(dialectOverrideType)
            .withMemberCategories(DECLARED_CLASSES)
            .test(hints));

    Set<String> declaredMemberClassNames =
        Arrays.stream(dialectOverrideType.getDeclaredClasses())
            .map(Class::getName)
            .collect(Collectors.toSet());
    assertEquals(
        declaredMemberClassNames,
        Set.copyOf(
            RatchetBeanFactoryInitializationAotProcessor
                .HIBERNATE_DIALECT_OVERRIDE_MEMBER_CLASS_NAMES));
    for (String className :
        RatchetBeanFactoryInitializationAotProcessor
            .HIBERNATE_DIALECT_OVERRIDE_MEMBER_CLASS_NAMES) {
      assertTrue(reflection().onType(Class.forName(className)).test(hints), className);
    }
    for (String className :
        RatchetBeanFactoryInitializationAotProcessor.HIBERNATE_DIALECT_OVERRIDE_ARRAY_CLASS_NAMES) {
      assertTrue(reflection().onType(TypeReference.of(className)).test(hints), className);
    }

    Class<?> overridesAnnotationType =
        Class.forName(
            RatchetBeanFactoryInitializationAotProcessor
                .HIBERNATE_DIALECT_OVERRIDE_META_ANNOTATION_CLASS_NAME);
    assertTrue(
        reflection()
            .onMethodInvocation(overridesAnnotationType.getDeclaredMethod("value"))
            .test(hints));
    assertTrue(
        reflection()
            .onType(Class.forName("org.hibernate.annotations.DialectOverride$SQLInsert"))
            .test(hints));
  }

  @Test
  void registersRatchetOrmXmlEntityConverterAndListenerReflection() throws Exception {
    RuntimeHints hints = process(beanFactory("")).hints();

    for (String className :
        RatchetBeanFactoryInitializationAotProcessor.RATCHET_JPA_ENTITY_CLASS_NAMES) {
      assertTrue(reflection().onType(Class.forName(className)).test(hints), className);
    }
    Map<String, Set<String>> entityFields =
        RatchetBeanFactoryInitializationAotProcessor.RATCHET_JPA_ENTITY_MEMBER_HINTS.stream()
            .collect(
                Collectors.toMap(
                    RatchetBeanFactoryInitializationAotProcessor.EntityMemberHint::className,
                    memberHint -> Set.copyOf(memberHint.fieldNames())));
    for (String className :
        RatchetBeanFactoryInitializationAotProcessor.RATCHET_JPA_ENTITY_CLASS_NAMES) {
      var typeHint = typeHint(hints, className);
      assertTrue(typeHint.getMemberCategories().isEmpty(), className);
      assertEquals(entityFields.getOrDefault(className, Set.of()), hintFieldNames(typeHint));
      assertEquals(entityFields.containsKey(className) ? 1 : 0, typeHint.constructors().count());
    }

    for (String className :
        RatchetBeanFactoryInitializationAotProcessor.RATCHET_JPA_CONVERTER_CLASS_NAMES) {
      assertTrue(reflection().onType(Class.forName(className)).test(hints), className);
      var typeHint = typeHint(hints, className);
      assertTrue(typeHint.getMemberCategories().isEmpty(), className);
      assertEquals(
          RatchetBeanFactoryInitializationAotProcessor.RATCHET_JPA_CONVERTER_CONSTRUCTOR_CLASS_NAMES
                  .contains(className)
              ? 1
              : 0,
          typeHint.constructors().count(),
          className);
    }
    Class<?> listenerType = Class.forName("run.ratchet.store.id.UuidV7EntityListener");
    assertTrue(
        reflection().onConstructorInvocation(listenerType.getDeclaredConstructor()).test(hints));
    assertTrue(
        reflection()
            .onMethodInvocation(listenerType.getMethod("assignId", Object.class))
            .test(hints));
  }

  @Test
  void skipsAllCuratedHibernateHintsWhenJpaGuardIsUnavailable() throws Exception {
    DefaultListableBeanFactory beanFactory = beanFactory("");
    ClassLoader testClassLoader = getClass().getClassLoader();
    beanFactory.setBeanClassLoader(
        new ClassLoader(testClassLoader) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (RatchetBeanFactoryInitializationAotProcessor.HIBERNATE_LOG_HELPER_CLASS_NAME.equals(
                name)) {
              throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
          }
        });

    RuntimeHints hints = process(beanFactory).hints();

    assertTrue(hibernateHintClassNames(hints).isEmpty());
    assertFalse(resource().forResource("org/hibernate/hibernate-mapping-3.0.dtd").test(hints));
    assertFalse(resource().forResource("org/hibernate/xsd/mapping/mapping-7.0.xsd").test(hints));
    assertFalse(
        reflection()
            .onType(Class.forName("org.hibernate.boot.jaxb.mapping.spi.JaxbEntityMappingsImpl"))
            .test(hints));
    assertFalse(
        reflection().onConstructorInvocation(ArrayList.class.getDeclaredConstructor()).test(hints));
    assertFalse(
        reflection().onType(Types.class).withMemberCategories(ACCESS_PUBLIC_FIELDS).test(hints));
    for (String methodName :
        List.of("getAnnotatedReceiverType", "getParameterCount", "getParameters")) {
      assertFalse(
          reflection().onMethodInvocation(Executable.class.getMethod(methodName)).test(hints),
          methodName);
    }
    for (String methodName : List.of("getModifiers", "isNamePresent")) {
      assertFalse(
          reflection().onMethodInvocation(Parameter.class.getMethod(methodName)).test(hints),
          methodName);
    }
    for (String methodName : List.of("getActualTypeArguments", "getRawType")) {
      assertFalse(
          reflection()
              .onMethodInvocation(ParameterizedType.class.getMethod(methodName))
              .test(hints),
          methodName);
    }
    for (String methodName : List.of("getLowerBounds", "getUpperBounds")) {
      assertFalse(
          reflection().onMethodInvocation(WildcardType.class.getMethod(methodName)).test(hints),
          methodName);
    }
    assertFalse(
        reflection()
            .onConstructorInvocation(ReflectPermission.class.getConstructor(String.class))
            .test(hints));
    assertFalse(
        reflection().onType(Class.forName("org.hibernate.boot.models.JpaAnnotations")).test(hints));
    Class<?> dialectOverrideType =
        Class.forName(
            RatchetBeanFactoryInitializationAotProcessor.HIBERNATE_DIALECT_OVERRIDE_CLASS_NAME);
    assertFalse(
        reflection()
            .onType(dialectOverrideType)
            .withMemberCategories(DECLARED_CLASSES)
            .test(hints));
    assertFalse(
        reflection()
            .onType(Class.forName("org.hibernate.annotations.DialectOverride$SQLInsert"))
            .test(hints));
    assertFalse(
        reflection()
            .onType(TypeReference.of("org.hibernate.annotations.DialectOverride$SQLInsert[]"))
            .test(hints));
    assertFalse(
        reflection()
            .onType(TypeReference.of(HIBERNATE_AUTO_FLUSH_EVENT_LISTENER_ARRAY))
            .test(hints));
    assertFalse(
        reflection().onType(TypeReference.of(HIBERNATE_LOAD_EVENT_LISTENER_ARRAY)).test(hints));
    assertTrue(hibernateEventListenerArrayHintClassNames(hints).isEmpty());
    assertFalse(reflection().onType(TypeReference.of(RATCHET_STRING_IDENTIFIER_ARRAY)).test(hints));
    assertFalse(reflection().onType(TypeReference.of(RATCHET_UUID_IDENTIFIER_ARRAY)).test(hints));
    Class<?> overridesAnnotationType =
        Class.forName(
            RatchetBeanFactoryInitializationAotProcessor
                .HIBERNATE_DIALECT_OVERRIDE_META_ANNOTATION_CLASS_NAME);
    assertFalse(
        reflection()
            .onMethodInvocation(overridesAnnotationType.getDeclaredMethod("value"))
            .test(hints));
    assertFalse(
        reflection()
            .onType(Class.forName("org.hibernate.dialect.type.PostgreSQLInetJdbcType"))
            .test(hints));
    assertFalse(
        reflection().onType(Class.forName("run.ratchet.store.entity.JobEntity")).test(hints));
    assertFalse(
        reflection()
            .onMethodInvocation(jakarta.persistence.CollectionTable.class.getMethod("options"))
            .test(hints));
    assertFalse(
        reflection()
            .onMethodInvocation(jakarta.xml.bind.annotation.XmlElement.class.getMethod("type"))
            .test(hints));
    Class<?> converterType = Class.forName("run.ratchet.store.converter.JsonMapConverter");
    assertFalse(
        reflection().onConstructorInvocation(converterType.getDeclaredConstructor()).test(hints));
    Class<?> listenerType = Class.forName("run.ratchet.store.id.UuidV7EntityListener");
    assertFalse(
        reflection()
            .onMethodInvocation(listenerType.getMethod("assignId", Object.class))
            .test(hints));
  }

  @Test
  void skipsOptionalLibraryHintsWhenTheirSpecificGuardsAreUnavailable() throws Exception {
    DefaultListableBeanFactory beanFactory = beanFactory("");
    Set<String> hiddenClasses =
        Set.of(
            "org.glassfish.jaxb.core.v2.model.nav.ReflectionNavigator",
            "org.postgresql.Driver",
            "run.ratchet.store.spi.RatchetEntityManagerProvider");
    ClassLoader testClassLoader = getClass().getClassLoader();
    beanFactory.setBeanClassLoader(
        new ClassLoader(testClassLoader) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (hiddenClasses.contains(name)) {
              throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
          }
        });

    RuntimeHints hints = process(beanFactory).hints();

    assertTrue(resource().forResource("org/hibernate/hibernate-mapping-3.0.dtd").test(hints));
    assertTrue(
        reflection().onType(Class.forName("org.hibernate.boot.models.JpaAnnotations")).test(hints));
    assertFalse(
        reflection()
            .onType(Class.forName("org.hibernate.boot.jaxb.mapping.spi.JaxbEntityMappingsImpl"))
            .test(hints));
    assertFalse(
        reflection()
            .onType(Class.forName("org.hibernate.dialect.type.PostgreSQLInetJdbcType"))
            .test(hints));
    assertFalse(
        reflection().onType(Class.forName("run.ratchet.store.entity.JobEntity")).test(hints));
    assertFalse(reflection().onType(TypeReference.of(RATCHET_STRING_IDENTIFIER_ARRAY)).test(hints));
    assertFalse(reflection().onType(TypeReference.of(RATCHET_UUID_IDENTIFIER_ARRAY)).test(hints));
  }

  @Test
  void skipsPostgresqlProviderHintsWhenDriverGuardIsUnavailable() throws Exception {
    DefaultListableBeanFactory beanFactory = beanFactory("");
    beanFactory.setBeanClassLoader(classLoaderWithout("org.postgresql.Driver"));

    RuntimeHints hints = process(beanFactory).hints();

    assertPostgresqlProviderHints(hints, false);
    assertHikariPoolHints(hints, true);
  }

  @Test
  void skipsHikariPoolHintsWhenDataSourceGuardIsUnavailable() throws Exception {
    DefaultListableBeanFactory beanFactory = beanFactory("");
    beanFactory.setBeanClassLoader(classLoaderWithout("com.zaxxer.hikari.HikariDataSource"));

    RuntimeHints hints = process(beanFactory).hints();

    assertHikariPoolHints(hints, false);
    assertPostgresqlProviderHints(hints, true);
  }

  @Test
  void emptyAllowlistDisablesTargetsButKeepsSubmitterAndInternalHints() throws IOException {
    DefaultListableBeanFactory beanFactory = beanFactory("");
    beanFactory.registerBeanDefinition("submitter", new RootBeanDefinition(JobSubmitter.class));

    ProcessingResult result = process(beanFactory);

    assertFalse(reflection().onType(JobSubmitter.class).test(result.hints()));
    assertLambdaHints(result.hints(), JobSubmitter.class);
    assertTrue(resource().forResource(classResource(JobSubmitter.class)).test(result.hints()));
    assertTrue(
        reflection()
            .onMethod(Executors.class, "newVirtualThreadPerTaskExecutor")
            .test(result.hints()));
    String manifest =
        result
            .files()
            .getGeneratedFileContent(
                Kind.RESOURCE, RatchetBeanFactoryInitializationAotProcessor.AOT_MANIFEST_RESOURCE);
    assertTrue(manifest.contains(JobSubmitter.class.getName() + "\n"));
  }

  @Test
  void emptyAllowlistWithoutSubmittersEmitsEnforcingManifestAndInternalHints() throws IOException {
    ProcessingResult result = process(beanFactory(""));

    assertTrue(
        reflection()
            .onMethod(Executors.class, "newVirtualThreadPerTaskExecutor")
            .test(result.hints()));
    assertEquals(
        "# ratchet-aot-manifest v1\n",
        result
            .files()
            .getGeneratedFileContent(
                Kind.RESOURCE, RatchetBeanFactoryInitializationAotProcessor.AOT_MANIFEST_RESOURCE));
    assertFalse(
        result
            .files()
            .getGeneratedFiles(Kind.RESOURCE)
            .containsKey(RatchetBeanFactoryInitializationAotProcessor.LAMBDA_METADATA_RESOURCE));
  }

  @Test
  void scansAnnotatedAbstractSubmittersFromApplicationRootsOutsideTheTargetAllowlist() {
    DefaultListableBeanFactory beanFactory = beanFactory("com.example.jobtargets");
    AutoConfigurationPackages.register(beanFactory, "run.ratchet.spring.boot.aot");

    RuntimeHints hints = process(beanFactory).hints();

    assertLambdaHints(hints, ExplicitSubmitter.class);
    assertLambdaHints(hints, AbstractExplicitSubmitter.class);
  }

  private static boolean payloadReflection(Class<?> type, RuntimeHints hints) {
    return reflection()
        .onType(type)
        .withMemberCategories(
            INVOKE_DECLARED_CONSTRUCTORS, INVOKE_DECLARED_METHODS, ACCESS_DECLARED_FIELDS)
        .test(hints);
  }

  private static void assertPostgresqlProviderHints(RuntimeHints hints, boolean expected)
      throws Exception {
    assertEquals(
        expected,
        reflection()
            .onConstructorInvocation(
                Class.forName("org.postgresql.Driver").getDeclaredConstructor())
            .test(hints));
    assertEquals(
        expected,
        resource()
            .forResource(
                RatchetBeanFactoryInitializationAotProcessor.POSTGRESQL_DRIVER_CONFIG_RESOURCE)
            .test(hints));
    Map<String, List<String>> fieldHints =
        Map.of(
            "org.postgresql.core.QueryExecutorCloseAction",
            List.of("pgStream"),
            "org.postgresql.jdbc.PgStatement",
            List.of("cancelTimerTask", "isClosed", "statementState"),
            "org.postgresql.util.PGobject",
            List.of("type", "value"));
    for (var fieldHint : fieldHints.entrySet()) {
      Class<?> type = Class.forName(fieldHint.getKey());
      for (String fieldName : fieldHint.getValue()) {
        assertEquals(
            expected,
            reflection().onFieldAccess(type.getDeclaredField(fieldName)).test(hints),
            fieldHint.getKey() + "." + fieldName);
      }
    }
    assertPostgresqlStoreProxyHints(hints, expected);
  }

  private static void assertPostgresqlStoreProxyHints(RuntimeHints hints, boolean expected)
      throws Exception {
    Class<?> storeType = Class.forName("run.ratchet.store.postgresql.PostgresqlJobStore");
    List<String> expectedProxyInterfaces =
        List.of(
            storeType.getName(),
            "org.springframework.aop.SpringProxy",
            "org.springframework.aop.framework.Advised",
            "org.springframework.core.DecoratingProxy");
    assertEquals(
        expectedProxyInterfaces,
        Arrays.stream(AopProxyUtils.completeJdkProxyInterfaces(storeType))
            .map(Class::getName)
            .toList());
    List<List<String>> storeProxyHints =
        hints
            .proxies()
            .jdkProxyHints()
            .map(hint -> hint.getProxiedInterfaces().stream().map(TypeReference::getName).toList())
            .filter(
                interfaces ->
                    !interfaces.isEmpty() && interfaces.get(0).equals(storeType.getName()))
            .toList();
    assertEquals(expected ? List.of(expectedProxyInterfaces) : List.of(), storeProxyHints);

    Class<?> implementationType =
        Class.forName("run.ratchet.store.postgresql.PostgresqlJobStoreImpl");
    assertEquals(
        expected,
        reflection()
            .onType(implementationType)
            .withMemberCategories(INVOKE_DECLARED_METHODS)
            .test(hints));
    for (Class<?> interfaceType : interfaceHierarchy(storeType)) {
      assertEquals(
          expected,
          reflection()
              .onType(interfaceType)
              .withMemberCategories(INVOKE_DECLARED_METHODS)
              .test(hints),
          interfaceType.getName());
    }
  }

  private static Set<Class<?>> interfaceHierarchy(Class<?> rootInterface) {
    Set<Class<?>> interfaces = new LinkedHashSet<>();
    addInterfaceHierarchy(rootInterface, interfaces);
    return interfaces;
  }

  private static void addInterfaceHierarchy(Class<?> type, Set<Class<?>> interfaces) {
    if (!type.isInterface() || !interfaces.add(type)) {
      return;
    }
    Arrays.stream(type.getInterfaces())
        .forEach(superInterface -> addInterfaceHierarchy(superInterface, interfaces));
  }

  private static void assertHikariPoolHints(RuntimeHints hints, boolean expected) throws Exception {
    for (String className :
        List.of("com.zaxxer.hikari.pool.PoolBase", "com.zaxxer.hikari.pool.PoolEntry")) {
      assertEquals(
          expected,
          reflection()
              .onType(Class.forName(className))
              .withMemberCategories(ACCESS_DECLARED_FIELDS)
              .test(hints),
          className);
    }
    assertEquals(
        expected,
        reflection().onType(TypeReference.of("java.sql.Statement[]")).test(hints),
        "java.sql.Statement[]");
    assertEquals(
        expected,
        reflection()
            .onType(TypeReference.of("com.zaxxer.hikari.util.ConcurrentBag$IConcurrentBagEntry[]"))
            .test(hints),
        "com.zaxxer.hikari.util.ConcurrentBag$IConcurrentBagEntry[]");
  }

  private static void assertLambdaHints(RuntimeHints hints, Class<?> declaringClass) {
    var lambdaHints = lambdaHints(hints, declaringClass);
    assertEquals(6, lambdaHints.size());
    assertTrue(lambdaHints.stream().allMatch(hint -> hint.getInterfaces().size() == 1));
    Set<String> interfaces =
        lambdaHints.stream()
            .flatMap(hint -> hint.getInterfaces().stream())
            .map(TypeReference::getName)
            .collect(Collectors.toSet());
    assertEquals(
        Set.of(
            SerializableBiConsumer.class.getName(),
            SerializableCheckedConsumer.class.getName(),
            SerializableCheckedRunnable.class.getName(),
            SerializableConsumer.class.getName(),
            SerializableFunction.class.getName(),
            SerializablePredicate.class.getName()),
        interfaces);
  }

  private static List<org.springframework.aot.hint.LambdaHint> lambdaHints(
      RuntimeHints hints, Class<?> declaringClass) {
    return hints
        .reflection()
        .lambdaHints()
        .filter(hint -> hint.getDeclaringClass().getName().equals(declaringClass.getName()))
        .toList();
  }

  private static List<String> hibernateGeneratedHintClassNames(RuntimeHints hints, String suffix) {
    return hibernateHintClassNames(hints).stream()
        .filter(className -> className.endsWith(suffix))
        .toList();
  }

  private static org.springframework.aot.hint.TypeHint typeHint(
      RuntimeHints hints, String className) {
    return hints
        .reflection()
        .typeHints()
        .filter(hint -> hint.getType().getName().equals(className))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing reflection hint for " + className));
  }

  private static Set<String> hintFieldNames(org.springframework.aot.hint.TypeHint typeHint) {
    return typeHint
        .fields()
        .map(org.springframework.aot.hint.FieldHint::getName)
        .collect(Collectors.toSet());
  }

  private static List<String> hibernateMappingModelHintClassNames(RuntimeHints hints) {
    return hibernateHintClassNames(hints).stream()
        .filter(className -> className.startsWith(HIBERNATE_MAPPING_MODEL_PREFIX))
        .toList();
  }

  private static List<String> hibernateEventListenerArrayHintClassNames(RuntimeHints hints) {
    return hibernateHintClassNames(hints).stream()
        .filter(className -> className.startsWith("org.hibernate.event.spi."))
        .filter(className -> className.endsWith("EventListener[]"))
        .toList();
  }

  private static List<String> hibernateStrategyHintClassNames(RuntimeHints hints) {
    return hibernateHintClassNames(hints).stream()
        .filter(className -> !className.endsWith("_$logger"))
        .filter(className -> !className.endsWith("_$bundle"))
        .toList();
  }

  private static List<String> hibernateHintClassNames(RuntimeHints hints) {
    return hints
        .reflection()
        .typeHints()
        .map(typeHint -> typeHint.getType().getName())
        .filter(className -> className.startsWith("org.hibernate."))
        .toList();
  }

  private static int occurrences(String value, String needle) {
    return value.split(java.util.regex.Pattern.quote(needle), -1).length - 1;
  }

  private static Class<?> loadClass(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException exception) {
      throw new IllegalStateException(exception);
    }
  }

  private static String classResource(Class<?> type) {
    return type.getName().replace('.', '/') + ".class";
  }

  private ClassLoader classLoaderWithout(String hiddenClassName) {
    ClassLoader testClassLoader = getClass().getClassLoader();
    return new ClassLoader(testClassLoader) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (hiddenClassName.equals(name)) {
          throw new ClassNotFoundException(name);
        }
        return super.loadClass(name, resolve);
      }
    };
  }

  private static DefaultListableBeanFactory beanFactory(String allowedPackages) {
    StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test",
                Map.of(
                    RatchetBeanFactoryInitializationAotProcessor.ALLOWED_PACKAGES_PROPERTY,
                    allowedPackages)));
    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.registerSingleton("environment", environment);
    return beanFactory;
  }

  private static ProcessingResult process(DefaultListableBeanFactory beanFactory) {
    RatchetBeanFactoryInitializationAotProcessor processor =
        new RatchetBeanFactoryInitializationAotProcessor();
    BeanFactoryInitializationAotContribution contribution =
        processor.processAheadOfTime(beanFactory);
    RuntimeHints hints = new RuntimeHints();
    InMemoryGeneratedFiles files = new InMemoryGeneratedFiles();
    contribution.applyTo(new HintsGenerationContext(hints, files), null);
    return new ProcessingResult(hints, files);
  }

  static final class JobTarget {
    public JobTarget() {}

    public void execute() {}
  }

  static final class JobSubmitter {
    JobSubmitter(JobSchedulerService schedulerService) {}

    public void submit() {}
  }

  static final class OrdinaryMethodParameter {
    public void inspect(JobSchedulerService schedulerService) {}
  }

  abstract static class InjectingSubmitterBase {
    JobSchedulerService schedulerService;

    public void submitFromBase() {}
  }

  static final class InheritedSubmitter extends InjectingSubmitterBase {}

  @RegisterJobSubmitter
  public static final class ExplicitSubmitter {
    public void submitViaLookup() {}
  }

  @RegisterJobSubmitter
  public abstract static class AbstractExplicitSubmitter {
    public void submitFromBaseViaLookup() {}
  }

  public static final class PayloadTarget {
    public PayloadResult execute(PayloadRequest request) {
      return new PayloadResult(request.nested().value());
    }
  }

  public interface DefaultJobContract {
    default InterfacePayload defaultJob(InterfacePayload payload) {
      return payload;
    }
  }

  public static final class InterfaceTarget implements DefaultJobContract {}

  public record InterfacePayload(String value) {}

  public record PayloadRequest(NestedPayload nested, CyclicPayload cyclic, URI excluded) {}

  public record NestedPayload(String value) {}

  public record PayloadResult(String value) {}

  public static final class CyclicPayload {
    private CyclicPayload next;

    public CyclicPayload getNext() {
      return next;
    }

    public void setNext(CyclicPayload next) {
      this.next = next;
    }
  }

  public static class RecurringBase {
    @Recurring(cron = "0 * * * * ?")
    public void recurringJob() {}
  }

  public static final class RecurringTarget extends RecurringBase {}

  private record ProcessingResult(RuntimeHints hints, InMemoryGeneratedFiles files) {}

  private record HintsGenerationContext(RuntimeHints hints, GeneratedFiles files)
      implements GenerationContext {

    @Override
    public GeneratedClasses getGeneratedClasses() {
      throw new UnsupportedOperationException();
    }

    @Override
    public GeneratedFiles getGeneratedFiles() {
      return files;
    }

    @Override
    public RuntimeHints getRuntimeHints() {
      return hints;
    }

    @Override
    public GenerationContext withName(String name) {
      return this;
    }
  }
}
