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

import java.io.IOException;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.ReflectPermission;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.hint.ExecutableMode;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.TypeReference;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotContribution;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotProcessor;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.Environment;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;
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

/**
 * Discovers Ratchet job submission and invocation types from finalized Spring bean definitions.
 *
 * <p>The processor loads application classes but never asks the bean factory for application bean
 * instances. Discovery is bounded by Ratchet's configured invocation package allowlist.
 */
public final class RatchetBeanFactoryInitializationAotProcessor
    implements BeanFactoryInitializationAotProcessor {

  static final String ALLOWED_PACKAGES_PROPERTY = "ratchet.class-policy.allowed-packages";
  static final String AOT_MANIFEST_RESOURCE = "META-INF/ratchet/aot-registered-classes.txt";
  static final String LAMBDA_METADATA_RESOURCE =
      "META-INF/native-image/run.ratchet/ratchet-spring-boot-aot-lambda/"
          + "reachability-metadata.json";
  static final String JPA_MAPPING_RESOURCE = "META-INF/orm.xml";
  static final String STORE_CORE_ANCHOR_RESOURCE =
      "run/ratchet/store/spi/RatchetEntityManagerProvider.class";
  static final String SCHEMA_MIGRATION_INDEX_RESOURCE = "ddl/migrations/index.txt";
  static final String SCHEMA_MIGRATION_SCRIPT_RESOURCE_PATTERN = "ddl/migrations/*.sql";
  static final String POSTGRESQL_DRIVER_CONFIG_RESOURCE = "org/postgresql/driverconfig.properties";
  static final String HIBERNATE_LOG_HELPER_CLASS_NAME = "org.hibernate.jpa.internal.util.LogHelper";
  static final List<String> HIBERNATE_XML_RESOURCE_PATTERNS =
      List.of("org/hibernate/*.dtd", "org/hibernate/**/*.xsd");
  static final String HIBERNATE_LOGGER_RESOURCE_PATTERN =
      "classpath*:org/hibernate/**/*_$logger.class";
  static final String HIBERNATE_BUNDLE_RESOURCE_PATTERN =
      "classpath*:org/hibernate/**/*_$bundle.class";
  static final String HIBERNATE_MAPPING_MODEL_RESOURCE_PATTERN =
      "classpath*:org/hibernate/boot/jaxb/mapping/spi/Jaxb*Impl*.class";
  static final String HIBERNATE_UNTRACED_MAPPING_MODEL_CLASS_NAME =
      "org.hibernate.boot.jaxb.mapping.spi.JaxbMultiTenancyImpl";
  static final List<String> RATCHET_JPA_ENTITY_CLASS_NAMES =
      List.of(
          "run.ratchet.store.entity.ArchivedJobEntity",
          "run.ratchet.store.entity.BatchEntity",
          "run.ratchet.store.entity.BatchMetricsEntity",
          "run.ratchet.store.entity.JobEntity",
          "run.ratchet.store.entity.JobExecutionEntity",
          "run.ratchet.store.entity.JobLogEntity",
          "run.ratchet.store.entity.NodeEntity",
          "run.ratchet.store.entity.ResourceLimitEntity",
          "run.ratchet.store.entity.ResourcePermitEntity",
          "run.ratchet.store.entity.WorkflowConditionEntity");
  static final List<String> RATCHET_JPA_CONVERTER_CLASS_NAMES =
      List.of(
          "run.ratchet.store.converter.JobPayloadConverter",
          "run.ratchet.store.converter.JobPriorityConverter",
          "run.ratchet.store.converter.JsonListConverter",
          "run.ratchet.store.converter.JsonMapConverter",
          "run.ratchet.store.converter.JsonObjectMapConverter");
  static final List<EntityMemberHint> RATCHET_JPA_ENTITY_MEMBER_HINTS =
      List.of(
          entityMemberHint("BatchEntity", "version"),
          entityMemberHint("BatchMetricsEntity", "version"),
          entityMemberHint("JobEntity", "version"),
          entityMemberHint(
              "JobExecutionEntity",
              "attempt",
              "durationMs",
              "endedAt",
              "errorClass",
              "errorMessage",
              "id",
              "jobId",
              "nodeId",
              "startedAt",
              "status"),
          entityMemberHint("NodeEntity", "id", "lastHeartbeat", "nodeInfo", "startedAt"));
  static final List<String> RATCHET_JPA_CONVERTER_CONSTRUCTOR_CLASS_NAMES =
      List.of(
          "run.ratchet.store.converter.JobPayloadConverter",
          "run.ratchet.store.converter.JobPriorityConverter",
          "run.ratchet.store.converter.JsonMapConverter",
          "run.ratchet.store.converter.JsonObjectMapConverter");
  static final List<String> HIBERNATE_ANNOTATION_CATALOG_CLASS_NAMES =
      List.of(
          "org.hibernate.boot.models.DialectOverrideAnnotations",
          "org.hibernate.boot.models.HibernateAnnotations",
          "org.hibernate.boot.models.JpaAnnotations",
          "org.hibernate.boot.models.XmlAnnotations");
  static final String HIBERNATE_DIALECT_OVERRIDE_CLASS_NAME =
      "org.hibernate.annotations.DialectOverride";
  static final String HIBERNATE_DIALECT_OVERRIDE_META_ANNOTATION_CLASS_NAME =
      HIBERNATE_DIALECT_OVERRIDE_CLASS_NAME + "$OverridesAnnotation";
  static final List<String> HIBERNATE_DIALECT_OVERRIDE_MEMBER_CLASS_NAMES =
      List.of(
          dialectOverrideMember("Version"),
          dialectOverrideMember("Check"),
          dialectOverrideMember("Checks"),
          dialectOverrideMember("SQLOrder"),
          dialectOverrideMember("SQLOrders"),
          dialectOverrideMember("ColumnDefault"),
          dialectOverrideMember("ColumnDefaults"),
          dialectOverrideMember("GeneratedColumn"),
          dialectOverrideMember("GeneratedColumns"),
          dialectOverrideMember("DiscriminatorFormula"),
          dialectOverrideMember("DiscriminatorFormulas"),
          dialectOverrideMember("Formula"),
          dialectOverrideMember("Formulas"),
          dialectOverrideMember("JoinFormula"),
          dialectOverrideMember("JoinFormulas"),
          dialectOverrideMember("SQLRestriction"),
          dialectOverrideMember("SQLRestrictions"),
          dialectOverrideMember("Filters"),
          dialectOverrideMember("FilterOverrides"),
          dialectOverrideMember("FilterDefs"),
          dialectOverrideMember("FilterDefOverrides"),
          dialectOverrideMember("SQLSelect"),
          dialectOverrideMember("SQLSelects"),
          dialectOverrideMember("SQLInsert"),
          dialectOverrideMember("SQLInserts"),
          dialectOverrideMember("SQLUpdate"),
          dialectOverrideMember("SQLUpdates"),
          dialectOverrideMember("SQLDelete"),
          dialectOverrideMember("SQLDeletes"),
          dialectOverrideMember("SQLDeleteAll"),
          dialectOverrideMember("SQLDeleteAlls"),
          HIBERNATE_DIALECT_OVERRIDE_META_ANNOTATION_CLASS_NAME);
  static final List<String> HIBERNATE_DIALECT_OVERRIDE_ARRAY_CLASS_NAMES =
      List.of(
          dialectOverrideArray("Check"),
          dialectOverrideArray("SQLOrder"),
          dialectOverrideArray("ColumnDefault"),
          dialectOverrideArray("GeneratedColumn"),
          dialectOverrideArray("DiscriminatorFormula"),
          dialectOverrideArray("Formula"),
          dialectOverrideArray("JoinFormula"),
          dialectOverrideArray("SQLRestriction"),
          dialectOverrideArray("Filters"),
          dialectOverrideArray("FilterDefs"),
          dialectOverrideArray("SQLSelect"),
          dialectOverrideArray("SQLInsert"),
          dialectOverrideArray("SQLUpdate"),
          dialectOverrideArray("SQLDelete"),
          dialectOverrideArray("SQLDeleteAll"));
  static final List<String> HIBERNATE_POSTGRESQL_JDBC_TYPE_CLASS_NAMES =
      List.of(
          "org.hibernate.dialect.type.PostgreSQLInetJdbcType",
          "org.hibernate.dialect.type.PostgreSQLIntervalSecondJdbcType",
          "org.hibernate.dialect.type.PostgreSQLJsonArrayPGObjectJsonbJdbcTypeConstructor",
          "org.hibernate.dialect.type.PostgreSQLJsonPGObjectJsonbType",
          "org.hibernate.dialect.type.PostgreSQLStructPGObjectJdbcType");
  private static final String HIBERNATE_MODELS_CONTEXT_CLASS_NAME =
      "org.hibernate.models.spi.ModelsContext";
  private static final String HIBERNATE_ANNOTATION_WRAPPER_PACKAGE =
      "org.hibernate.boot.models.annotations.internal.";
  static final List<ConstructorHint> HIBERNATE_ANNOTATION_WRAPPER_CONSTRUCTORS =
      List.of(
          annotationWrapper("AccessJpaAnnotation"),
          annotationWrapper("CacheAnnotation"),
          annotationWrapper("CollectionTableJpaAnnotation", "jakarta.persistence.CollectionTable"),
          annotationWrapper("ColumnJpaAnnotation", "jakarta.persistence.Column"),
          annotationWrapper("ConvertJpaAnnotation", "jakarta.persistence.Convert"),
          annotationWrapper(
              "ElementCollectionJpaAnnotation", "jakarta.persistence.ElementCollection"),
          annotationWrapper("EntityJpaAnnotation", "jakarta.persistence.Entity"),
          annotationWrapper("EntityListenersJpaAnnotation", "jakarta.persistence.EntityListeners"),
          annotationWrapper("EnumeratedJpaAnnotation", "jakarta.persistence.Enumerated"),
          annotationWrapper("ForeignKeyJpaAnnotation", "jakarta.persistence.ForeignKey"),
          annotationWrapper("IdJpaAnnotation", "jakarta.persistence.Id"),
          annotationWrapper("IndexJpaAnnotation", "jakarta.persistence.Index"),
          annotationWrapper("JoinColumnJpaAnnotation", "jakarta.persistence.JoinColumn"),
          annotationWrapper("ManyToOneJpaAnnotation", "jakarta.persistence.ManyToOne"),
          annotationWrapper("MapsIdJpaAnnotation", "jakarta.persistence.MapsId"),
          annotationWrapper("OneToOneJpaAnnotation", "jakarta.persistence.OneToOne"),
          annotationWrapper("PrePersistJpaAnnotation", "jakarta.persistence.PrePersist"),
          annotationWrapper("PreUpdateJpaAnnotation", "jakarta.persistence.PreUpdate"),
          annotationWrapper("TableJpaAnnotation", "jakarta.persistence.Table"),
          annotationWrapper("TransientJpaAnnotation", "jakarta.persistence.Transient"),
          annotationWrapper("VersionJpaAnnotation", "jakarta.persistence.Version"));
  private static final String HIBERNATE_BOOTSTRAP_REGISTRY_BUILDER_CLASS_NAME =
      "org.hibernate.boot.registry.BootstrapServiceRegistryBuilder";
  private static final String HIBERNATE_MAPPING_BINDER_CLASS_NAME =
      "org.hibernate.boot.jaxb.internal.MappingBinder";
  private static final String JAXB_REFLECTION_NAVIGATOR_CLASS_NAME =
      "org.glassfish.jaxb.core.v2.model.nav.ReflectionNavigator";
  private static final String STORE_CORE_ANCHOR_CLASS_NAME =
      "run.ratchet.store.spi.RatchetEntityManagerProvider";
  private static final String RATCHET_ENTITY_LISTENER_CLASS_NAME =
      "run.ratchet.store.id.UuidV7EntityListener";
  private static final String POSTGRESQL_DRIVER_CLASS_NAME = "org.postgresql.Driver";
  private static final String POSTGRESQL_JOB_STORE_CLASS_NAME =
      "run.ratchet.store.postgresql.PostgresqlJobStore";
  private static final String POSTGRESQL_JOB_STORE_IMPL_CLASS_NAME =
      "run.ratchet.store.postgresql.PostgresqlJobStoreImpl";
  private static final String HIKARI_DATA_SOURCE_CLASS_NAME = "com.zaxxer.hikari.HikariDataSource";
  private static final String HIBERNATE_STRATEGY_SELECTOR_CLASS_NAME =
      "org.hibernate.boot.registry.selector.spi.StrategySelector";
  private static final String HIBERNATE_EVENT_TYPE_CLASS_NAME = "org.hibernate.event.spi.EventType";
  private static final Set<String> JPA_IDENTIFIER_ANNOTATION_CLASS_NAMES =
      Set.of("jakarta.persistence.EmbeddedId", "jakarta.persistence.Id");
  private static final String HIBERNATE_STRATEGY_REGISTRATION_CLASS_NAME =
      "org.hibernate.boot.registry.selector.StrategyRegistration";
  private static final String HIBERNATE_STRATEGY_REGISTRATION_PROVIDER_CLASS_NAME =
      "org.hibernate.boot.registry.selector.StrategyRegistrationProvider";

  private static final int MAX_PAYLOAD_DEPTH = 8;
  private static final String ENVIRONMENT_BEAN_NAME = "environment";
  private static final String JOB_PAYLOAD_CLASS_NAME = "run.ratchet.store.entity.JobPayload";
  private static final Log log =
      LogFactory.getLog(RatchetBeanFactoryInitializationAotProcessor.class);
  private static final List<Class<?>> LAMBDA_INTERFACES =
      List.of(
          SerializableBiConsumer.class,
          SerializableCheckedConsumer.class,
          SerializableCheckedRunnable.class,
          SerializableConsumer.class,
          SerializableFunction.class,
          SerializablePredicate.class);
  private static final MemberCategory[] INVOCATION_MEMBER_CATEGORIES = {
    MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
    MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS,
    MemberCategory.INVOKE_DECLARED_METHODS,
    MemberCategory.INVOKE_PUBLIC_METHODS
  };
  private static final MemberCategory[] PAYLOAD_MEMBER_CATEGORIES = {
    MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
    MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS,
    MemberCategory.INVOKE_DECLARED_METHODS,
    MemberCategory.INVOKE_PUBLIC_METHODS,
    MemberCategory.ACCESS_DECLARED_FIELDS,
    MemberCategory.ACCESS_PUBLIC_FIELDS
  };
  private static final MemberCategory[] HIBERNATE_LOGGER_MEMBER_CATEGORIES = {
    MemberCategory.INVOKE_DECLARED_CONSTRUCTORS
  };
  private static final MemberCategory[] HIBERNATE_BUNDLE_MEMBER_CATEGORIES = {
    MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
    MemberCategory.ACCESS_DECLARED_FIELDS,
    MemberCategory.ACCESS_PUBLIC_FIELDS
  };
  private static final MemberCategory[] HIBERNATE_STRATEGY_MEMBER_CATEGORIES = {
    MemberCategory.INVOKE_DECLARED_CONSTRUCTORS
  };
  // Hibernate exposes implementors by role, but does not expose the registered role set.
  private static final List<String> HIBERNATE_STRATEGY_ROLE_CLASS_NAMES =
      List.of(
          "org.hibernate.boot.model.naming.ImplicitNamingStrategy",
          "org.hibernate.boot.model.relational.ColumnOrderingStrategy",
          "org.hibernate.cache.spi.CacheKeysFactory",
          "org.hibernate.id.enhanced.ImplicitDatabaseObjectNamingStrategy",
          "org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy",
          "org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy",
          "org.hibernate.resource.transaction.spi.TransactionCoordinatorBuilder",
          "org.hibernate.type.format.FormatMapper");
  // Keep metadata-building defaults explicit even though the selector inventory also finds them.
  private static final List<String> HIBERNATE_REQUIRED_STRATEGY_CLASS_NAMES =
      List.of(
          "org.hibernate.boot.model.naming.ImplicitNamingStrategyJpaCompliantImpl",
          "org.hibernate.boot.model.relational.ColumnOrderingStrategyLegacy",
          "org.hibernate.boot.model.relational.ColumnOrderingStrategyStandard");
  // The public implementor API deliberately rejects Hibernate's lazy Dialect and JtaPlatform roles.
  private static final List<String> HIBERNATE_LAZY_STRATEGY_CLASS_NAMES =
      List.of(
          "org.hibernate.dialect.CockroachDialect",
          "org.hibernate.dialect.DB2Dialect",
          "org.hibernate.dialect.DB2iDialect",
          "org.hibernate.dialect.DB2zDialect",
          "org.hibernate.dialect.H2Dialect",
          "org.hibernate.dialect.HANADialect",
          "org.hibernate.dialect.HSQLDialect",
          "org.hibernate.dialect.MariaDBDialect",
          "org.hibernate.dialect.MySQLDialect",
          "org.hibernate.dialect.OracleDialect",
          "org.hibernate.dialect.PostgresPlusDialect",
          "org.hibernate.dialect.PostgreSQLDialect",
          "org.hibernate.dialect.SQLServerDialect",
          "org.hibernate.dialect.SpannerDialect",
          "org.hibernate.dialect.SybaseASEDialect",
          "org.hibernate.dialect.SybaseDialect",
          "org.hibernate.engine.transaction.jta.platform.internal.AtomikosJtaPlatform",
          "org.hibernate.engine.transaction.jta.platform.internal.GlassFishJtaPlatform",
          "org.hibernate.engine.transaction.jta.platform.internal.JBossAppServerJtaPlatform",
          "org.hibernate.engine.transaction.jta.platform.internal.JBossStandAloneJtaPlatform",
          "org.hibernate.engine.transaction.jta.platform.internal.NarayanaJtaPlatform",
          "org.hibernate.engine.transaction.jta.platform.internal.ResinJtaPlatform",
          "org.hibernate.engine.transaction.jta.platform.internal.WebSphereLibertyJtaPlatform",
          "org.hibernate.engine.transaction.jta.platform.internal.WeblogicJtaPlatform",
          "org.hibernate.engine.transaction.jta.platform.internal.WildFlyStandAloneJtaPlatform");
  private static final Set<Class<?>> JDK_VALUE_TYPES =
      Set.of(
          String.class,
          Boolean.class,
          Byte.class,
          Character.class,
          Short.class,
          Integer.class,
          Long.class,
          Float.class,
          Double.class,
          BigDecimal.class,
          BigInteger.class,
          UUID.class,
          Optional.class,
          Collection.class,
          List.class,
          Set.class,
          Map.class);

  @Override
  public BeanFactoryInitializationAotContribution processAheadOfTime(
      ConfigurableListableBeanFactory beanFactory) {
    Environment environment = springEnvironment(beanFactory);
    Set<String> allowedPackages = allowedPackages(environment);
    if (allowedPackages.isEmpty()) {
      log.info(
          "Ratchet AOT application discovery is disabled because "
              + ALLOWED_PACKAGES_PROPERTY
              + " is empty; submitter discovery and internal hints remain enabled");
    }

    ClassLoader classLoader = beanFactory.getBeanClassLoader();
    boolean postgresqlPresent = ClassUtils.isPresent(POSTGRESQL_DRIVER_CLASS_NAME, classLoader);
    Class<?> postgresqlJobStoreType =
        postgresqlPresent ? loadClass(POSTGRESQL_JOB_STORE_CLASS_NAME, classLoader) : null;
    boolean hikariPresent = ClassUtils.isPresent(HIKARI_DATA_SOURCE_CLASS_NAME, classLoader);
    HibernateRuntimeHints hibernateRuntimeHints =
        discoverHibernateRuntimeHints(classLoader, postgresqlPresent);
    Set<Class<?>> allBeanClasses = new LinkedHashSet<>();
    Arrays.stream(beanFactory.getBeanDefinitionNames())
        .sorted()
        .map(beanFactory::getBeanDefinition)
        .filter(definition -> !definition.isAbstract() && definition.isAutowireCandidate())
        .map(definition -> resolveBeanClass(definition, classLoader))
        .filter(type -> type != null && isStableUserClass(type))
        .forEach(allBeanClasses::add);

    Set<Class<?>> applicationBeanClasses = new LinkedHashSet<>();
    Set<Class<?>> submitterClasses = new LinkedHashSet<>();
    allBeanClasses.stream()
        .filter(type -> isAllowed(type, allowedPackages))
        .forEach(applicationBeanClasses::add);
    allBeanClasses.forEach(type -> submitterClasses.addAll(injectedSubmitterClasses(type)));
    submitterClasses.addAll(
        annotatedSubmitterClasses(
            environment, classLoader, submitterScanPackages(beanFactory, allowedPackages)));

    Set<Method> applicationMethods = applicationMethods(applicationBeanClasses);
    Set<Method> recurringMethods = recurringMethods(applicationBeanClasses);
    PayloadTypeWalker payloadTypes = new PayloadTypeWalker(allowedPackages);
    applicationBeanClasses.forEach(payloadTypes::walkRootClass);
    applicationMethods.forEach(payloadTypes::walkMethod);
    recurringMethods.forEach(payloadTypes::walkMethod);

    Set<Class<?>> manifestClasses = new LinkedHashSet<>(applicationBeanClasses);
    manifestClasses.addAll(payloadTypes.applicationTypes());
    manifestClasses.addAll(submitterClasses);
    return new RatchetAotContribution(
        applicationBeanClasses,
        submitterClasses,
        recurringMethods,
        payloadTypes.reflectionTypes(),
        manifestClasses,
        hibernateRuntimeHints,
        postgresqlJobStoreType,
        postgresqlPresent,
        hikariPresent);
  }

  private static HibernateRuntimeHints discoverHibernateRuntimeHints(
      ClassLoader classLoader, boolean postgresqlPresent) {
    if (!ClassUtils.isPresent(HIBERNATE_LOG_HELPER_CLASS_NAME, classLoader)) {
      return HibernateRuntimeHints.NONE;
    }

    PathMatchingResourcePatternResolver resolver =
        new PathMatchingResourcePatternResolver(classLoader);
    CachingMetadataReaderFactory metadataReaderFactory = new CachingMetadataReaderFactory(resolver);
    boolean xmlBinderPresent =
        ClassUtils.isPresent(HIBERNATE_MAPPING_BINDER_CLASS_NAME, classLoader)
            && ClassUtils.isPresent(JAXB_REFLECTION_NAVIGATOR_CLASS_NAME, classLoader);
    List<Class<?>> mappingModelClasses =
        xmlBinderPresent
            ? generatedClasses(
                    resolver,
                    metadataReaderFactory,
                    classLoader,
                    HIBERNATE_MAPPING_MODEL_RESOURCE_PATTERN)
                .stream()
                // The tracing run queried every Hibernate 7.4.1 mapping implementation except
                // this unused multi-tenancy model type.
                .filter(type -> !type.getName().equals(HIBERNATE_UNTRACED_MAPPING_MODEL_CLASS_NAME))
                .toList()
            : List.of();
    boolean ratchetJpaStorePresent =
        ClassUtils.isPresent(STORE_CORE_ANCHOR_CLASS_NAME, classLoader);
    return new HibernateRuntimeHints(
        true,
        xmlBinderPresent,
        ratchetJpaStorePresent,
        postgresqlPresent,
        generatedClasses(
            resolver, metadataReaderFactory, classLoader, HIBERNATE_LOGGER_RESOURCE_PATTERN),
        generatedClasses(
            resolver, metadataReaderFactory, classLoader, HIBERNATE_BUNDLE_RESOURCE_PATTERN),
        hibernateEventListenerArrayClasses(classLoader),
        ratchetJpaStorePresent ? ratchetEntityIdentifierArrayClasses(classLoader) : List.of(),
        hibernateStrategyClasses(classLoader),
        mappingModelClasses);
  }

  private static List<Class<?>> hibernateEventListenerArrayClasses(ClassLoader classLoader) {
    Set<Class<?>> listenerArrayClasses = new TreeSet<>(Comparator.comparing(Class::getName));
    try {
      Class<?> eventType = ClassUtils.forName(HIBERNATE_EVENT_TYPE_CLASS_NAME, classLoader);
      Object values = eventType.getMethod("values").invoke(null);
      if (!(values instanceof Collection<?> standardEventTypes)) {
        throw new IllegalStateException("Hibernate returned a non-collection event type inventory");
      }
      Method baseListenerInterface = eventType.getMethod("baseListenerInterface");
      for (Object standardEventType : standardEventTypes) {
        if (!eventType.isInstance(standardEventType)) {
          throw new IllegalStateException(
              "Hibernate returned a non-EventType standard event: "
                  + standardEventType.getClass().getName());
        }
        Object listenerType = baseListenerInterface.invoke(standardEventType);
        if (!(listenerType instanceof Class<?> listenerClass)) {
          throw new IllegalStateException(
              "Hibernate returned a non-class base listener interface for " + standardEventType);
        }
        listenerArrayClasses.add(Array.newInstance(listenerClass, 0).getClass());
      }
    } catch (Exception | LinkageError exception) {
      throw new IllegalStateException(
          "Failed to discover Hibernate event listener arrays", exception);
    }
    return List.copyOf(listenerArrayClasses);
  }

  private static List<Class<?>> ratchetEntityIdentifierArrayClasses(ClassLoader classLoader) {
    Set<Class<?>> identifierArrayClasses = new TreeSet<>(Comparator.comparing(Class::getName));
    try {
      for (String entityClassName : RATCHET_JPA_ENTITY_CLASS_NAMES) {
        Class<?> entityClass = ClassUtils.forName(entityClassName, classLoader);
        List<Class<?>> identifierTypes = new ArrayList<>();
        for (Class<?> current = entityClass;
            current != null && current != Object.class;
            current = current.getSuperclass()) {
          Arrays.stream(current.getDeclaredFields())
              .filter(RatchetBeanFactoryInitializationAotProcessor::isJpaIdentifier)
              .map(Field::getType)
              .forEach(identifierTypes::add);
          Arrays.stream(current.getDeclaredMethods())
              .filter(RatchetBeanFactoryInitializationAotProcessor::isJpaIdentifier)
              .map(Method::getReturnType)
              .forEach(identifierTypes::add);
        }
        if (identifierTypes.size() != 1) {
          throw new IllegalStateException(
              "Expected exactly one JPA identifier member on "
                  + entityClassName
                  + ", found "
                  + identifierTypes.size());
        }
        identifierArrayClasses.add(Array.newInstance(identifierTypes.get(0), 0).getClass());
      }
    } catch (ClassNotFoundException | LinkageError exception) {
      throw new IllegalStateException(
          "Failed to discover Ratchet entity identifier arrays", exception);
    }
    return List.copyOf(identifierArrayClasses);
  }

  private static boolean isJpaIdentifier(AnnotatedElement member) {
    return Arrays.stream(member.getDeclaredAnnotations())
        .map(annotation -> annotation.annotationType().getName())
        .anyMatch(JPA_IDENTIFIER_ANNOTATION_CLASS_NAMES::contains);
  }

  private static List<Class<?>> hibernateStrategyClasses(ClassLoader classLoader) {
    Set<Class<?>> strategyRoles = new TreeSet<>(Comparator.comparing(Class::getName));
    Set<Class<?>> strategyClasses = new TreeSet<>(Comparator.comparing(Class::getName));
    try {
      for (String className : HIBERNATE_STRATEGY_ROLE_CLASS_NAMES) {
        strategyRoles.add(ClassUtils.forName(className, classLoader));
      }
      strategyRoles.addAll(strategyRegistrationProviderRoles(classLoader));

      try (AutoCloseable registry = hibernateBootstrapRegistry(classLoader)) {
        Class<?> strategySelectorType =
            ClassUtils.forName(HIBERNATE_STRATEGY_SELECTOR_CLASS_NAME, classLoader);
        Object strategySelector =
            registry
                .getClass()
                .getMethod("getService", Class.class)
                .invoke(registry, strategySelectorType);
        Method registeredImplementors =
            strategySelectorType.getMethod("getRegisteredStrategyImplementors", Class.class);
        for (Class<?> strategyRole : strategyRoles) {
          Object registered = registeredImplementors.invoke(strategySelector, strategyRole);
          if (!(registered instanceof Collection<?> implementations)) {
            throw new IllegalStateException(
                "Hibernate returned a non-collection strategy inventory for "
                    + strategyRole.getName());
          }
          for (Object implementation : implementations) {
            if (!(implementation instanceof Class<?> implementationClass)) {
              throw new IllegalStateException(
                  "Hibernate returned a non-class strategy implementation for "
                      + strategyRole.getName());
            }
            strategyClasses.add(implementationClass);
          }
        }
      }

      for (String className : HIBERNATE_REQUIRED_STRATEGY_CLASS_NAMES) {
        strategyClasses.add(ClassUtils.forName(className, classLoader));
      }
      for (String className : HIBERNATE_LAZY_STRATEGY_CLASS_NAMES) {
        strategyClasses.add(ClassUtils.forName(className, classLoader));
      }
    } catch (Exception | LinkageError exception) {
      throw new IllegalStateException(
          "Failed to discover Hibernate strategy implementations", exception);
    }
    return List.copyOf(strategyClasses);
  }

  private static Set<Class<?>> strategyRegistrationProviderRoles(ClassLoader classLoader)
      throws ReflectiveOperationException {
    Class<?> providerType =
        ClassUtils.forName(HIBERNATE_STRATEGY_REGISTRATION_PROVIDER_CLASS_NAME, classLoader);
    Class<?> registrationType =
        ClassUtils.forName(HIBERNATE_STRATEGY_REGISTRATION_CLASS_NAME, classLoader);
    Method getRegistrations = providerType.getMethod("getStrategyRegistrations");
    Method getStrategyRole = registrationType.getMethod("getStrategyRole");
    Set<Class<?>> strategyRoles = new TreeSet<>(Comparator.comparing(Class::getName));
    for (Object provider : ServiceLoader.load(providerType, classLoader)) {
      Object registrations = getRegistrations.invoke(provider);
      if (!(registrations instanceof Iterable<?> iterable)) {
        throw new IllegalStateException(
            "Hibernate strategy registration provider returned a non-iterable inventory: "
                + provider.getClass().getName());
      }
      for (Object registration : iterable) {
        Object strategyRole = getStrategyRole.invoke(registration);
        if (!(strategyRole instanceof Class<?> strategyRoleClass)) {
          throw new IllegalStateException(
              "Hibernate strategy registration returned a non-class strategy role: "
                  + registration.getClass().getName());
        }
        strategyRoles.add(strategyRoleClass);
      }
    }
    return strategyRoles;
  }

  private static AutoCloseable hibernateBootstrapRegistry(ClassLoader classLoader)
      throws ReflectiveOperationException {
    Class<?> registryBuilderType =
        ClassUtils.forName(HIBERNATE_BOOTSTRAP_REGISTRY_BUILDER_CLASS_NAME, classLoader);
    Object registryBuilder = registryBuilderType.getConstructor().newInstance();
    registryBuilderType
        .getMethod("applyClassLoader", ClassLoader.class)
        .invoke(registryBuilder, classLoader);
    Object registry = registryBuilderType.getMethod("build").invoke(registryBuilder);
    if (!(registry instanceof AutoCloseable closeableRegistry)) {
      throw new IllegalStateException(
          "Hibernate bootstrap service registry is not closeable: "
              + registry.getClass().getName());
    }
    return closeableRegistry;
  }

  private static List<Class<?>> generatedClasses(
      PathMatchingResourcePatternResolver resolver,
      CachingMetadataReaderFactory metadataReaderFactory,
      ClassLoader classLoader,
      String resourcePattern) {
    Set<Class<?>> classes = new TreeSet<>(Comparator.comparing(Class::getName));
    try {
      for (Resource resource : resolver.getResources(resourcePattern)) {
        String className =
            metadataReaderFactory.getMetadataReader(resource).getClassMetadata().getClassName();
        try {
          classes.add(ClassUtils.forName(className, classLoader));
        } catch (ClassNotFoundException | LinkageError exception) {
          throw new IllegalStateException(
              "Failed to load discovered Hibernate generated class " + className, exception);
        }
      }
    } catch (IOException exception) {
      throw new IllegalStateException(
          "Failed to discover Hibernate generated classes matching " + resourcePattern, exception);
    }
    return List.copyOf(classes);
  }

  private static Environment springEnvironment(ConfigurableListableBeanFactory beanFactory) {
    Object environmentSingleton = beanFactory.getSingleton(ENVIRONMENT_BEAN_NAME);
    if (environmentSingleton instanceof Environment environment) {
      return environment;
    }
    log.warn(
        "Ratchet AOT application discovery cannot read the Spring Environment; treating the "
            + "invocation allowlist as empty");
    return null;
  }

  private static Set<String> allowedPackages(Environment environment) {
    if (environment == null) {
      return Set.of();
    }
    List<String> configured =
        Binder.get(environment)
            .bind(ALLOWED_PACKAGES_PROPERTY, Bindable.listOf(String.class))
            .orElse(List.of());
    Set<String> normalized = new LinkedHashSet<>();
    for (String candidate : configured) {
      if (candidate == null || candidate.isBlank()) {
        continue;
      }
      String trimmed = candidate.trim();
      normalized.add(trimmed.endsWith(".") ? trimmed : trimmed + ".");
    }
    return Collections.unmodifiableSet(normalized);
  }

  private static Class<?> resolveBeanClass(BeanDefinition definition, ClassLoader classLoader) {
    ResolvableType resolvableType = definition.getResolvableType();
    Class<?> resolved = resolvableType != ResolvableType.NONE ? resolvableType.resolve() : null;
    if (resolved == null && definition.getBeanClassName() != null) {
      resolved = loadClass(definition.getBeanClassName(), classLoader);
    }
    return resolved != null ? ClassUtils.getUserClass(resolved) : null;
  }

  private static Class<?> loadClass(String className, ClassLoader classLoader) {
    try {
      return ClassUtils.forName(className, classLoader);
    } catch (ClassNotFoundException | LinkageError exception) {
      log.debug("Skipping unresolved application class " + className, exception);
      return null;
    }
  }

  private static Set<Class<?>> annotatedSubmitterClasses(
      Environment environment, ClassLoader classLoader, Set<String> scanPackages) {
    if (environment == null || scanPackages.isEmpty()) {
      return Set.of();
    }
    ClassPathScanningCandidateComponentProvider scanner =
        new ClassPathScanningCandidateComponentProvider(false, environment) {
          @Override
          protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
            return beanDefinition.getMetadata().isIndependent();
          }
        };
    scanner.addIncludeFilter(new AnnotationTypeFilter(RegisterJobSubmitter.class));
    scanner.setResourceLoader(new DefaultResourceLoader(classLoader));
    Set<Class<?>> annotated = new LinkedHashSet<>();
    scanPackages.stream()
        .sorted()
        .forEach(
            basePackage ->
                scanner.findCandidateComponents(basePackage).stream()
                    .map(BeanDefinition::getBeanClassName)
                    .filter(className -> className != null)
                    .map(className -> loadClass(className, classLoader))
                    .filter(type -> type != null && isStableUserClass(type))
                    .forEach(annotated::add));
    return annotated;
  }

  private static Set<String> submitterScanPackages(
      ConfigurableListableBeanFactory beanFactory, Set<String> allowedPackages) {
    Set<String> packages = new LinkedHashSet<>();
    allowedPackages.stream()
        .map(prefix -> prefix.substring(0, prefix.length() - 1))
        .forEach(packages::add);
    if (AutoConfigurationPackages.has(beanFactory)) {
      AutoConfigurationPackages.get(beanFactory).stream()
          .filter(packageName -> packageName != null && !packageName.isBlank())
          .forEach(packages::add);
    }
    return packages;
  }

  private static boolean isStableUserClass(Class<?> type) {
    return !type.isSynthetic() && !Proxy.isProxyClass(type);
  }

  private static boolean isAllowed(Class<?> type, Set<String> allowedPackages) {
    String className = type.getName();
    return allowedPackages.stream().anyMatch(className::startsWith);
  }

  private static Set<Class<?>> injectedSubmitterClasses(Class<?> type) {
    Set<Class<?>> submitters = new LinkedHashSet<>();
    for (Class<?> current = type;
        current != null && current != Object.class;
        current = current.getSuperclass()) {
      boolean declaresInjectionPoint = false;
      for (Field field : current.getDeclaredFields()) {
        if (field.getType() == JobSchedulerService.class) {
          declaresInjectionPoint = true;
        }
      }
      for (Method method : current.getDeclaredMethods()) {
        for (Class<?> parameterType : method.getParameterTypes()) {
          if (parameterType == JobSchedulerService.class) {
            declaresInjectionPoint = true;
          }
        }
      }
      for (Constructor<?> constructor : current.getDeclaredConstructors()) {
        for (Class<?> parameterType : constructor.getParameterTypes()) {
          if (parameterType == JobSchedulerService.class) {
            declaresInjectionPoint = true;
          }
        }
      }
      if (declaresInjectionPoint) {
        submitters.add(current);
      }
    }
    if (!submitters.isEmpty()) {
      submitters.add(type);
    }
    return submitters;
  }

  private static Set<Method> applicationMethods(Set<Class<?>> applicationBeanClasses) {
    Set<Method> methods = new LinkedHashSet<>();
    for (Class<?> type : applicationBeanClasses) {
      Arrays.stream(type.getMethods())
          .filter(method -> !method.isSynthetic() && !method.isBridge())
          .forEach(methods::add);
      for (Class<?> current = type;
          current != null && current != Object.class;
          current = current.getSuperclass()) {
        Arrays.stream(current.getDeclaredMethods())
            .filter(method -> !method.isSynthetic() && !method.isBridge())
            .forEach(methods::add);
      }
    }
    return methods;
  }

  private static Set<Method> recurringMethods(Set<Class<?>> applicationBeanClasses) {
    Set<Method> methods = new LinkedHashSet<>();
    for (Class<?> type : applicationBeanClasses) {
      for (Class<?> current = type;
          current != null && current != Object.class;
          current = current.getSuperclass()) {
        Arrays.stream(current.getDeclaredMethods())
            .filter(method -> !method.isSynthetic())
            .filter(method -> !method.isBridge())
            .filter(method -> method.isAnnotationPresent(Recurring.class))
            .forEach(methods::add);
      }
    }
    return methods;
  }

  private record RatchetAotContribution(
      Set<Class<?>> applicationBeanClasses,
      Set<Class<?>> submitterClasses,
      Set<Method> recurringMethods,
      Set<Class<?>> payloadReflectionTypes,
      Set<Class<?>> manifestClasses,
      HibernateRuntimeHints hibernateRuntimeHints,
      Class<?> postgresqlJobStoreType,
      boolean postgresqlPresent,
      boolean hikariPresent)
      implements BeanFactoryInitializationAotContribution {

    private RatchetAotContribution {
      applicationBeanClasses = Set.copyOf(applicationBeanClasses);
      submitterClasses = Set.copyOf(submitterClasses);
      recurringMethods = Set.copyOf(recurringMethods);
      payloadReflectionTypes = Set.copyOf(payloadReflectionTypes);
      manifestClasses = Set.copyOf(manifestClasses);
    }

    @Override
    public void applyTo(
        GenerationContext generationContext,
        org.springframework.beans.factory.aot.BeanFactoryInitializationCode
            beanFactoryInitializationCode) {
      RuntimeHints hints = generationContext.getRuntimeHints();
      registerRuntimeResources(hints, hibernateRuntimeHints, postgresqlPresent);
      registerDatabaseProviderReflectionHints(
          hints, postgresqlPresent, hikariPresent, postgresqlJobStoreType);
      registerHibernateReflectionHints(hints, hibernateRuntimeHints);
      registerInternalHints(hints);
      applicationBeanClasses.forEach(type -> registerInvocationReflection(hints, type));
      payloadReflectionTypes.forEach(type -> registerPayloadReflection(hints, type));
      recurringMethods.forEach(
          method -> hints.reflection().registerMethod(method, ExecutableMode.INVOKE));
      submitterClasses.forEach(type -> registerSubmitterHints(hints, type));
      hints.resources().registerPattern(AOT_MANIFEST_RESOURCE);

      generationContext
          .getGeneratedFiles()
          .addResourceFile(AOT_MANIFEST_RESOURCE, manifestContent(manifestClasses));
      if (!submitterClasses.isEmpty()) {
        generationContext
            .getGeneratedFiles()
            .addResourceFile(LAMBDA_METADATA_RESOURCE, lambdaMetadataContent(submitterClasses));
      }
    }

    private static void registerInvocationReflection(RuntimeHints hints, Class<?> type) {
      hints.reflection().registerType(type, INVOCATION_MEMBER_CATEGORIES);
    }

    private static void registerPayloadReflection(RuntimeHints hints, Class<?> type) {
      hints.reflection().registerType(type, PAYLOAD_MEMBER_CATEGORIES);
    }

    private static void registerRuntimeResources(
        RuntimeHints hints,
        HibernateRuntimeHints hibernateRuntimeHints,
        boolean postgresqlPresent) {
      hints.resources().registerPattern(JPA_MAPPING_RESOURCE);
      hints.resources().registerPattern(STORE_CORE_ANCHOR_RESOURCE);
      hints.resources().registerPattern(SCHEMA_MIGRATION_INDEX_RESOURCE);
      hints.resources().registerPattern(SCHEMA_MIGRATION_SCRIPT_RESOURCE_PATTERN);
      if (postgresqlPresent) {
        // org.postgresql.Driver's default-property loader enumerates this classpath resource.
        hints.resources().registerPattern(POSTGRESQL_DRIVER_CONFIG_RESOURCE);
      }
      if (hibernateRuntimeHints.hibernatePresent()) {
        // LocalXmlResourceResolver, MappingXsdSupport, ConfigXsdSupport, and JaxbCfgProcessor
        // resolve Hibernate's bundled schemas from these two exact jar families.
        HIBERNATE_XML_RESOURCE_PATTERNS.forEach(hints.resources()::registerPattern);
      }
    }

    private static void registerDatabaseProviderReflectionHints(
        RuntimeHints hints,
        boolean postgresqlPresent,
        boolean hikariPresent,
        Class<?> postgresqlJobStoreType) {
      if (postgresqlPresent) {
        // Spring's DriverDataSource/DriverManager path invokes the configured driver's no-arg
        // constructor; buildpack builds do not consume pgjdbc's central-repository metadata.
        registerConstructor(hints, POSTGRESQL_DRIVER_CLASS_NAME, List.of());
        // QueryExecutorCloseAction's AtomicReferenceFieldUpdater targets pgStream on the Cleaner
        // action registered for abandoned connections.
        registerFields(hints, "org.postgresql.core.QueryExecutorCloseAction", List.of("pgStream"));
        // PgStatement's AtomicReference/AtomicIntegerFieldUpdaters target these three state fields.
        registerFields(
            hints,
            "org.postgresql.jdbc.PgStatement",
            List.of("cancelTimerTask", "isClosed", "statementState"));
        // pgjdbc's traced PGobject handling accesses its complete declared value state.
        registerFields(hints, "org.postgresql.util.PGobject", List.of("type", "value"));
        registerPostgresqlStoreProxyHints(hints, postgresqlJobStoreType);
      }
      if (hikariPresent) {
        // HikariCP's PoolBase state lookups require access to its declared connection state.
        hints
            .reflection()
            .registerType(
                TypeReference.of("com.zaxxer.hikari.pool.PoolBase"),
                MemberCategory.ACCESS_DECLARED_FIELDS);
        // HikariCP's AtomicIntegerFieldUpdater-backed PoolEntry state requires declared-field
        // access.
        hints
            .reflection()
            .registerType(
                TypeReference.of("com.zaxxer.hikari.pool.PoolEntry"),
                MemberCategory.ACCESS_DECLARED_FIELDS);
        // HikariCP 7.0.2's PoolEntry builds its openStatements FastList through
        // Array.newInstance(Statement.class, ...), so the array class itself must be registered.
        hints.reflection().registerType(TypeReference.of("java.sql.Statement[]"));
        // ConcurrentBag's default strong thread-local cache uses the same FastList constructor
        // with IConcurrentBagEntry.class on the connection-borrow path.
        hints
            .reflection()
            .registerType(
                TypeReference.of("com.zaxxer.hikari.util.ConcurrentBag$IConcurrentBagEntry[]"));
      }
    }

    private static void registerPostgresqlStoreProxyHints(
        RuntimeHints hints, Class<?> postgresqlJobStoreType) {
      if (postgresqlJobStoreType == null) {
        return;
      }
      hints
          .proxies()
          .registerJdkProxy(AopProxyUtils.completeJdkProxyInterfaces(postgresqlJobStoreType));
      hints
          .reflection()
          .registerType(
              TypeReference.of(POSTGRESQL_JOB_STORE_IMPL_CLASS_NAME),
              MemberCategory.INVOKE_DECLARED_METHODS);
      interfaceHierarchy(postgresqlJobStoreType)
          .forEach(
              type ->
                  hints.reflection().registerType(type, MemberCategory.INVOKE_DECLARED_METHODS));
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

    private static void registerHibernateReflectionHints(
        RuntimeHints hints, HibernateRuntimeHints hibernateRuntimeHints) {
      if (!hibernateRuntimeHints.hibernatePresent()) {
        return;
      }
      hibernateRuntimeHints
          .loggerClasses()
          .forEach(
              type -> hints.reflection().registerType(type, HIBERNATE_LOGGER_MEMBER_CATEGORIES));
      hibernateRuntimeHints
          .bundleClasses()
          .forEach(
              type -> hints.reflection().registerType(type, HIBERNATE_BUNDLE_MEMBER_CATEGORIES));
      // EventListenerGroupImpl.internalAppend() reaches createListenerArrayForWrite(), and
      // EventListenerRegistryImpl.resolveListenerInstances() uses the same Array.newInstance
      // family based on eventType.baseListenerInterface() for every standard event type.
      hibernateRuntimeHints
          .eventListenerArrayClasses()
          .forEach(type -> hints.reflection().registerType(type));
      // AbstractEntityPersister.prepareLoaders() constructs MultiIdEntityLoaderArrayParam for
      // array-capable dialects and reflectively allocates one array per entity identifier type;
      // array JDBC descriptor resolution reaches ReflectHelper.arrayClass() for the same family.
      hibernateRuntimeHints
          .ratchetEntityIdentifierArrayClasses()
          .forEach(type -> hints.reflection().registerType(type));
      hibernateRuntimeHints
          .strategyClasses()
          .forEach(
              type -> hints.reflection().registerType(type, HIBERNATE_STRATEGY_MEMBER_CATEGORIES));
      registerAdditionalHibernateReflectionHints(hints, hibernateRuntimeHints.postgresqlPresent());
      if (hibernateRuntimeHints.xmlBinderPresent()) {
        registerHibernateXmlBinderReflectionHints(
            hints, hibernateRuntimeHints.mappingModelClasses());
      }
      if (hibernateRuntimeHints.ratchetJpaStorePresent()) {
        registerRatchetJpaReflectionHints(hints);
      }
    }

    private static void registerAdditionalHibernateReflectionHints(
        RuntimeHints hints, boolean postgresqlPresent) {
      // OrmAnnotationHelper enumerates these generated descriptor registries with getFields().
      HIBERNATE_ANNOTATION_CATALOG_CLASS_NAMES.forEach(
          className ->
              hints
                  .reflection()
                  .registerType(TypeReference.of(className), MemberCategory.ACCESS_PUBLIC_FIELDS));
      // OrmAnnotationDescriptor selects these exact constructors while materializing the annotation
      // wrappers used by Ratchet's entity model.
      HIBERNATE_ANNOTATION_WRAPPER_CONSTRUCTORS.forEach(
          constructorHint ->
              registerConstructor(
                  hints, constructorHint.className(), constructorHint.parameterTypeNames()));

      // DialectOverridesAnnotationHelper.buildOverrideMap() calls getNestMembers() when
      // EntityBinder.resolveCustomSqlAnnotation() first initializes the helper for every entity.
      hints
          .reflection()
          .registerType(
              TypeReference.of(HIBERNATE_DIALECT_OVERRIDE_CLASS_NAME),
              MemberCategory.DECLARED_CLASSES);
      // The same helper queries isAnnotation() and @OverridesAnnotation on every returned nest
      // member before populating its base-annotation-to-override map.
      HIBERNATE_DIALECT_OVERRIDE_MEMBER_CLASS_NAMES.forEach(
          className -> hints.reflection().registerType(TypeReference.of(className)));
      // EntityBinder.resolveCustomSqlAnnotation() and getOverridableAnnotation() ask Hibernate
      // Models for repeated usages of each mapped override type. AnnotationUsageHelper allocates an
      // empty array reflectively even when no override is present, so each mapped array is needed.
      HIBERNATE_DIALECT_OVERRIDE_ARRAY_CLASS_NAMES.forEach(
          className -> hints.reflection().registerType(TypeReference.of(className)));
      // DialectOverridesAnnotationHelper.buildOverrideMap() invokes the meta-annotation value()
      // method to resolve each base annotation, including SQLInsert, SQLUpdate, and SQLDelete.
      hints
          .reflection()
          .registerType(
              TypeReference.of(HIBERNATE_DIALECT_OVERRIDE_META_ANNOTATION_CLASS_NAME),
              builder -> builder.withMethod("value", List.of(), ExecutableMode.INVOKE));

      // Hibernate's default scanner strategy is instantiated through the strategy selector.
      registerConstructor(
          hints, "org.hibernate.boot.archive.scan.internal.DisabledScanner", List.of());
      // PersisterFactoryImpl resolves and invokes these standard persister constructors.
      registerConstructor(
          hints,
          "org.hibernate.persister.collection.BasicCollectionPersister",
          List.of(
              "org.hibernate.mapping.Collection",
              "org.hibernate.cache.spi.access.CollectionDataAccess",
              "org.hibernate.metamodel.spi.RuntimeModelCreationContext"));
      registerConstructor(
          hints,
          "org.hibernate.persister.entity.SingleTableEntityPersister",
          List.of(
              "org.hibernate.mapping.PersistentClass",
              "org.hibernate.cache.spi.access.EntityDataAccess",
              "org.hibernate.cache.spi.access.NaturalIdDataAccess",
              "org.hibernate.metamodel.spi.RuntimeModelCreationContext"));
      // Service dependency injection reflectively invokes this @InjectService setter.
      hints
          .reflection()
          .registerType(
              TypeReference.of(
                  "org.hibernate.engine.jdbc.connections.internal.DataSourceConnectionProvider"),
              builder ->
                  builder.withMethod(
                      "setJndiService",
                      List.of(TypeReference.of("org.hibernate.engine.jndi.spi.JndiService")),
                      ExecutableMode.INVOKE));
      // EventType and JdbcTypeNameMapper enumerate these constant holders reflectively.
      hints
          .reflection()
          .registerType(
              TypeReference.of("org.hibernate.event.spi.EventType"),
              MemberCategory.ACCESS_PUBLIC_FIELDS);
      hints
          .reflection()
          .registerType(
              TypeReference.of("org.hibernate.type.SqlTypes"), MemberCategory.ACCESS_PUBLIC_FIELDS);

      if (postgresqlPresent) {
        // PostgreSQLDialect's PgJdbcHelper loads these types by name and invokes their no-arg
        // constructors only when the PostgreSQL driver is available.
        HIBERNATE_POSTGRESQL_JDBC_TYPE_CLASS_NAMES.forEach(
            className -> registerConstructor(hints, className, List.of()));
      }
    }

    private static void registerHibernateXmlBinderReflectionHints(
        RuntimeHints hints, List<Class<?>> mappingModelClasses) {
      // MappingBinder creates a JAXBContext over this complete observed Hibernate mapping-model
      // graph before unmarshalling META-INF/orm.xml.
      mappingModelClasses.forEach(type -> hints.reflection().registerType(type));
      // JAXB ClassFactory instantiates collection properties of the Jaxb*Impl mapping model.
      // Hibernate 7.4.1 declares only List collection properties, for which JAXB selects
      // ArrayList.
      registerConstructor(hints, ArrayList.class.getName(), List.of());
      // Hibernate enumerates java.sql.Types constants reflectively.
      hints
          .reflection()
          .registerType(TypeReference.of(Types.class), MemberCategory.ACCESS_PUBLIC_FIELDS);
      // The native-image tracing agent observed Executable methods on JAXB/Hibernate
      // property-introspection reflection paths.
      registerNoArgMethods(
          hints,
          Executable.class.getName(),
          List.of("getAnnotatedReceiverType", "getParameterCount", "getParameters"));
      // The native-image tracing agent observed Parameter methods on JAXB/Hibernate
      // property-introspection reflection paths.
      registerNoArgMethods(
          hints, Parameter.class.getName(), List.of("getModifiers", "isNamePresent"));
      // The native-image tracing agent observed ParameterizedType methods on JAXB/Hibernate
      // property-introspection reflection paths.
      registerNoArgMethods(
          hints,
          ParameterizedType.class.getName(),
          List.of("getActualTypeArguments", "getRawType"));
      // The native-image tracing agent observed WildcardType methods on JAXB/Hibernate
      // property-introspection reflection paths.
      registerNoArgMethods(
          hints, WildcardType.class.getName(), List.of("getLowerBounds", "getUpperBounds"));
      // The native-image tracing agent observed ReflectPermission construction on JAXB/Hibernate
      // property-introspection reflection paths.
      registerConstructor(
          hints, ReflectPermission.class.getName(), List.of(String.class.getName()));
      registerDefaultConstructorAndFields(
          hints, "org.hibernate.boot.jaxb.mapping.spi.JaxbConverterImpl", List.of("clazz"));
      registerDefaultConstructorAndFields(
          hints, "org.hibernate.boot.jaxb.mapping.spi.JaxbEntityImpl", List.of("clazz"));
      registerDefaultConstructorAndFields(
          hints,
          "org.hibernate.boot.jaxb.mapping.spi.JaxbEntityMappingsImpl",
          List.of("converters", "entities", "version"));
      registerFields(
          hints,
          "org.hibernate.boot.jaxb.mapping.spi.JaxbPluralFetchModeImpl",
          List.of("JOIN", "SELECT", "SUBSELECT"));
      registerFields(
          hints,
          "org.hibernate.boot.jaxb.mapping.spi.JaxbPolymorphismTypeImpl",
          List.of("EXPLICIT", "IMPLICIT"));
      registerFields(
          hints,
          "org.hibernate.boot.jaxb.mapping.spi.JaxbSingularFetchModeImpl",
          List.of("JOIN", "SELECT"));

      // Hibernate Models reflectively reads these Jakarta Persistence 3.2 annotation attributes
      // while it combines the annotation model with the unmarshalled XML mapping model.
      registerNoArgMethods(
          hints,
          "jakarta.persistence.CollectionTable",
          List.of("foreignKey", "indexes", "joinColumns", "options", "uniqueConstraints"));
      registerNoArgMethods(hints, "jakarta.persistence.Column", List.of("check"));
      registerNoArgMethods(hints, "jakarta.persistence.JoinColumn", List.of("check", "foreignKey"));
      registerNoArgMethods(
          hints, "jakarta.persistence.Table", List.of("check", "indexes", "uniqueConstraints"));

      // JAXB's runtime model navigator invokes these annotation/helper members while constructing
      // the BindingContext used by MappingBinder.
      registerNoArgMethods(hints, "jakarta.xml.bind.annotation.XmlElement", List.of("type"));
      registerNoArgMethods(hints, "jakarta.xml.bind.annotation.XmlEnum", List.of("value"));
      registerNoArgMethods(hints, "jakarta.xml.bind.annotation.XmlSeeAlso", List.of("value"));
      registerNoArgMethods(hints, "jakarta.xml.bind.annotation.XmlType", List.of("factoryClass"));
      registerNoArgMethods(
          hints,
          "jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter",
          List.of("type", "value"));
      hints
          .reflection()
          .registerType(
              TypeReference.of("jakarta.xml.bind.annotation.adapters.CollapsedStringAdapter"),
              builder -> builder.withConstructor(List.of(), ExecutableMode.INVOKE));
      registerNoArgMethods(hints, JAXB_REFLECTION_NAVIGATOR_CLASS_NAME, List.of("getInstance"));
    }

    private static void registerRatchetJpaReflectionHints(RuntimeHints hints) {
      // Hibernate resolves the ten Ratchet entities by the class names in META-INF/orm.xml;
      // Spring's generated JPA AOT metadata did not contribute these types.
      RATCHET_JPA_ENTITY_CLASS_NAMES.forEach(
          className -> hints.reflection().registerType(TypeReference.of(className)));
      // The trace recorded construction and field access for this exact entity subset.
      RATCHET_JPA_ENTITY_MEMBER_HINTS.forEach(
          memberHint ->
              registerDefaultConstructorAndFields(
                  hints, memberHint.className(), memberHint.fieldNames()));
      // The same orm.xml names all five converters; four were constructed reflectively in the
      // traced execution while JsonListConverter required type lookup only.
      RATCHET_JPA_CONVERTER_CLASS_NAMES.forEach(
          className -> hints.reflection().registerType(TypeReference.of(className)));
      RATCHET_JPA_CONVERTER_CONSTRUCTOR_CLASS_NAMES.forEach(
          className -> registerConstructor(hints, className, List.of()));
      // Entity callback dispatch constructs the listener and invokes its @PrePersist method.
      hints
          .reflection()
          .registerType(
              TypeReference.of(RATCHET_ENTITY_LISTENER_CLASS_NAME),
              builder ->
                  builder
                      .withConstructor(List.of(), ExecutableMode.INVOKE)
                      .withMethod(
                          "assignId",
                          List.of(TypeReference.of(Object.class)),
                          ExecutableMode.INVOKE));
    }

    private static void registerDefaultConstructorAndFields(
        RuntimeHints hints, String className, List<String> fieldNames) {
      hints
          .reflection()
          .registerType(
              TypeReference.of(className),
              builder -> {
                builder.withConstructor(List.of(), ExecutableMode.INVOKE);
                fieldNames.forEach(builder::withField);
              });
    }

    private static void registerFields(
        RuntimeHints hints, String className, List<String> fieldNames) {
      hints
          .reflection()
          .registerType(
              TypeReference.of(className), builder -> fieldNames.forEach(builder::withField));
    }

    private static void registerNoArgMethods(
        RuntimeHints hints, String className, List<String> methodNames) {
      hints
          .reflection()
          .registerType(
              TypeReference.of(className),
              builder ->
                  methodNames.forEach(
                      methodName ->
                          builder.withMethod(methodName, List.of(), ExecutableMode.INVOKE)));
    }

    private static void registerConstructor(
        RuntimeHints hints, String className, List<String> parameterTypeNames) {
      hints
          .reflection()
          .registerType(
              TypeReference.of(className),
              builder ->
                  builder.withConstructor(
                      parameterTypeNames.stream().map(TypeReference::of).toList(),
                      ExecutableMode.INVOKE));
    }

    private static void registerSubmitterHints(RuntimeHints hints, Class<?> declaringClass) {
      LAMBDA_INTERFACES.forEach(
          functionalInterface ->
              hints
                  .reflection()
                  .registerLambda(
                      declaringClass, builder -> builder.withInterfaces(functionalInterface)));
      hints.resources().registerType(declaringClass);
    }

    private static void registerInternalHints(RuntimeHints hints) {
      hints
          .reflection()
          .registerType(TypeReference.of(JOB_PAYLOAD_CLASS_NAME), PAYLOAD_MEMBER_CATEGORIES);
      try {
        hints
            .reflection()
            .registerMethod(
                RecurringMethodInvoker.class.getMethod(
                    "invoke", String.class, String.class, boolean.class),
                ExecutableMode.INVOKE);
        hints
            .reflection()
            .registerMethod(JobPlaceholders.class.getMethod("noop"), ExecutableMode.INVOKE);
      } catch (NoSuchMethodException exception) {
        throw new IllegalStateException("Ratchet AOT internal method contract changed", exception);
      }
      hints
          .reflection()
          .registerType(
              Executors.class,
              builder ->
                  builder
                      .withMethod(
                          "newVirtualThreadPerTaskExecutor", List.of(), ExecutableMode.INVOKE)
                      .withMethod(
                          "newCachedThreadPool",
                          List.of(TypeReference.of(ThreadFactory.class)),
                          ExecutableMode.INVOKE)
                      .withMethod(
                          "newScheduledThreadPool",
                          List.of(
                              TypeReference.of(int.class), TypeReference.of(ThreadFactory.class)),
                          ExecutableMode.INVOKE));
    }
  }

  private record HibernateRuntimeHints(
      boolean hibernatePresent,
      boolean xmlBinderPresent,
      boolean ratchetJpaStorePresent,
      boolean postgresqlPresent,
      List<Class<?>> loggerClasses,
      List<Class<?>> bundleClasses,
      List<Class<?>> eventListenerArrayClasses,
      List<Class<?>> ratchetEntityIdentifierArrayClasses,
      List<Class<?>> strategyClasses,
      List<Class<?>> mappingModelClasses) {

    private static final HibernateRuntimeHints NONE =
        new HibernateRuntimeHints(
            false, false, false, false, List.of(), List.of(), List.of(), List.of(), List.of(),
            List.of());

    private HibernateRuntimeHints {
      loggerClasses = List.copyOf(loggerClasses);
      bundleClasses = List.copyOf(bundleClasses);
      eventListenerArrayClasses = List.copyOf(eventListenerArrayClasses);
      ratchetEntityIdentifierArrayClasses = List.copyOf(ratchetEntityIdentifierArrayClasses);
      strategyClasses = List.copyOf(strategyClasses);
      mappingModelClasses = List.copyOf(mappingModelClasses);
    }
  }

  private static String manifestContent(Set<Class<?>> manifestClasses) {
    StringBuilder manifest = new StringBuilder("# ratchet-aot-manifest v1\n");
    manifestClasses.stream()
        .map(Class::getName)
        .sorted()
        .forEach(className -> manifest.append(className).append('\n'));
    return manifest.toString();
  }

  private static String lambdaMetadataContent(Set<Class<?>> submitterClasses) {
    List<LambdaMetadataEntry> entries = new ArrayList<>();
    submitterClasses.stream()
        .map(Class::getName)
        .sorted()
        .forEach(
            declaringClass ->
                LAMBDA_INTERFACES.stream()
                    .map(Class::getName)
                    .sorted()
                    .forEach(
                        functionalInterface ->
                            entries.add(
                                new LambdaMetadataEntry(declaringClass, functionalInterface))));

    StringBuilder json = new StringBuilder("{\n  \"reflection\": [\n");
    for (int index = 0; index < entries.size(); index++) {
      LambdaMetadataEntry entry = entries.get(index);
      json.append("    {\n")
          .append("      \"type\": {\n")
          .append("        \"lambda\": {\n")
          .append("          \"declaringClass\": \"")
          .append(entry.declaringClass())
          .append("\",\n")
          .append("          \"interfaces\": [\"")
          .append(entry.functionalInterface())
          .append("\"]\n")
          .append("        }\n")
          .append("      },\n")
          .append("      \"methods\": [\n")
          .append("        {\n")
          .append("          \"name\": \"writeReplace\",\n")
          .append("          \"parameterTypes\": []\n")
          .append("        }\n")
          .append("      ]\n")
          .append("    }");
      if (index + 1 < entries.size()) {
        json.append(',');
      }
      json.append('\n');
    }
    return json.append("  ]\n}\n").toString();
  }

  private record LambdaMetadataEntry(String declaringClass, String functionalInterface) {}

  private static ConstructorHint annotationWrapper(String simpleName) {
    return new ConstructorHint(
        HIBERNATE_ANNOTATION_WRAPPER_PACKAGE + simpleName,
        List.of(HIBERNATE_MODELS_CONTEXT_CLASS_NAME));
  }

  private static String dialectOverrideMember(String simpleName) {
    return HIBERNATE_DIALECT_OVERRIDE_CLASS_NAME + "$" + simpleName;
  }

  private static String dialectOverrideArray(String simpleName) {
    return dialectOverrideMember(simpleName) + "[]";
  }

  private static ConstructorHint annotationWrapper(String simpleName, String annotationClassName) {
    return new ConstructorHint(
        HIBERNATE_ANNOTATION_WRAPPER_PACKAGE + simpleName,
        List.of(annotationClassName, HIBERNATE_MODELS_CONTEXT_CLASS_NAME));
  }

  private static EntityMemberHint entityMemberHint(String simpleName, String... fieldNames) {
    return new EntityMemberHint("run.ratchet.store.entity." + simpleName, List.of(fieldNames));
  }

  record ConstructorHint(String className, List<String> parameterTypeNames) {

    ConstructorHint {
      parameterTypeNames = List.copyOf(parameterTypeNames);
    }
  }

  record EntityMemberHint(String className, List<String> fieldNames) {

    EntityMemberHint {
      fieldNames = List.copyOf(fieldNames);
    }
  }

  private static final class PayloadTypeWalker {
    private final Set<String> allowedPackages;
    private final Map<Class<?>, Integer> visitedDepth = new HashMap<>();
    private final Set<Class<?>> reflectionTypes = new LinkedHashSet<>();
    private final Set<Class<?>> applicationTypes = new LinkedHashSet<>();
    private final Set<Type> activeGenericTypes = new LinkedHashSet<>();

    private PayloadTypeWalker(Set<String> allowedPackages) {
      this.allowedPackages = allowedPackages;
    }

    private void walkRootClass(Class<?> type) {
      walk(type, 0);
    }

    private void walkMethod(Method method) {
      walk(method.getGenericReturnType(), 0);
      for (Type parameterType : method.getGenericParameterTypes()) {
        walk(parameterType, 0);
      }
    }

    private Set<Class<?>> reflectionTypes() {
      return Collections.unmodifiableSet(reflectionTypes);
    }

    private Set<Class<?>> applicationTypes() {
      return Collections.unmodifiableSet(applicationTypes);
    }

    private void walk(Type candidate, int depth) {
      if (candidate == null || depth > MAX_PAYLOAD_DEPTH || !activeGenericTypes.add(candidate)) {
        return;
      }
      try {
        if (candidate instanceof Class<?> type) {
          walkClass(type, depth);
        } else if (candidate instanceof ParameterizedType parameterizedType) {
          walk(parameterizedType.getRawType(), depth);
          for (Type argument : parameterizedType.getActualTypeArguments()) {
            walk(argument, depth + 1);
          }
        } else if (candidate instanceof GenericArrayType arrayType) {
          walk(arrayType.getGenericComponentType(), depth + 1);
        } else if (candidate instanceof WildcardType wildcardType) {
          for (Type upperBound : wildcardType.getUpperBounds()) {
            walk(upperBound, depth + 1);
          }
          for (Type lowerBound : wildcardType.getLowerBounds()) {
            walk(lowerBound, depth + 1);
          }
        } else if (candidate instanceof TypeVariable<?> variable) {
          for (Type bound : variable.getBounds()) {
            walk(bound, depth + 1);
          }
        }
      } finally {
        activeGenericTypes.remove(candidate);
      }
    }

    private void walkClass(Class<?> candidate, int depth) {
      Class<?> type = candidate;
      while (type.isArray()) {
        type = type.getComponentType();
      }
      if (type.isPrimitive() || type == void.class) {
        return;
      }
      boolean applicationType = isAllowed(type, allowedPackages);
      boolean jdkValueType = isJdkValueType(type);
      if (!applicationType && !jdkValueType) {
        return;
      }

      Integer priorDepth = visitedDepth.get(type);
      if (priorDepth != null && priorDepth <= depth) {
        return;
      }
      visitedDepth.put(type, depth);
      reflectionTypes.add(type);
      if (applicationType) {
        applicationTypes.add(type);
      }
      if (jdkValueType) {
        return;
      }

      Arrays.stream(type.getDeclaredFields())
          .filter(field -> !field.isSynthetic())
          .filter(field -> !Modifier.isStatic(field.getModifiers()))
          .map(Field::getGenericType)
          .forEach(fieldType -> walk(fieldType, depth + 1));
      Arrays.stream(type.getDeclaredMethods())
          .filter(method -> !method.isSynthetic() && !method.isBridge())
          .filter(PayloadTypeWalker::isPropertyMethod)
          .forEach(
              method -> {
                walk(method.getGenericReturnType(), depth + 1);
                for (Type parameterType : method.getGenericParameterTypes()) {
                  walk(parameterType, depth + 1);
                }
              });
      walk(type.getGenericSuperclass(), depth + 1);
      for (Type implementedInterface : type.getGenericInterfaces()) {
        walk(implementedInterface, depth + 1);
      }
    }

    private static boolean isPropertyMethod(Method method) {
      if (Modifier.isStatic(method.getModifiers())) {
        return false;
      }
      String name = method.getName();
      return (name.startsWith("get") && method.getParameterCount() == 0)
          || (name.startsWith("is") && method.getParameterCount() == 0)
          || (name.startsWith("set") && method.getParameterCount() == 1);
    }

    private static boolean isJdkValueType(Class<?> type) {
      if (JDK_VALUE_TYPES.contains(type) || type.getName().startsWith("java.time.")) {
        return true;
      }
      String packageName = type.getPackageName();
      return "java.util".equals(packageName)
          && (Collection.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type));
    }
  }
}
