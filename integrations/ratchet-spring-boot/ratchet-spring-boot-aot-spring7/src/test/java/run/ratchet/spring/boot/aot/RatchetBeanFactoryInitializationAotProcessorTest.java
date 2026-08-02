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
import static org.springframework.aot.hint.MemberCategory.INVOKE_DECLARED_CONSTRUCTORS;
import static org.springframework.aot.hint.MemberCategory.INVOKE_DECLARED_METHODS;
import static org.springframework.aot.hint.predicate.RuntimeHintsPredicates.reflection;
import static org.springframework.aot.hint.predicate.RuntimeHintsPredicates.resource;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
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

  private static int occurrences(String value, String needle) {
    return value.split(java.util.regex.Pattern.quote(needle), -1).length - 1;
  }

  private static String classResource(Class<?> type) {
    return type.getName().replace('.', '/') + ".class";
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
