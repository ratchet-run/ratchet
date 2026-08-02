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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
        manifestClasses);
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
      Set<Class<?>> manifestClasses)
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
