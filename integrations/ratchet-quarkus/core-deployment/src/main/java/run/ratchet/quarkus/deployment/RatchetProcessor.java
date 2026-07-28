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
package run.ratchet.quarkus.deployment;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.IndexDependencyBuildItem;
import io.quarkus.deployment.builditem.SystemPropertyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.LambdaCapturingTypeBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageSystemPropertyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.FieldInfo;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;
import run.ratchet.quarkus.runtime.QuarkusPrincipalSource;
import run.ratchet.quarkus.runtime.QuarkusRatchetExecutorProvider;
import run.ratchet.quarkus.runtime.RatchetRuntimeProducers;
import run.ratchet.quarkus.runtime.RatchetStartupTrigger;
import run.ratchet.ri.cdi.RatchetRuntimeStart;

/**
 * Shared build-time wiring for Ratchet Quarkus flavors. Persistence-specific deployment modules add
 * only the store-provider pieces for their flavor.
 */
class RatchetProcessor {

  private static final String FEATURE = "ratchet";
  private static final DotName JOB_SCHEDULER_SERVICE =
      DotName.createSimple("run.ratchet.api.JobSchedulerService");

  @BuildStep
  FeatureBuildItem feature() {
    return new FeatureBuildItem(FEATURE);
  }

  /**
   * The extension's own flavor-neutral beans: the StartupEvent trigger, the executor, and default
   * producers.
   */
  @BuildStep
  AdditionalBeanBuildItem beans() {
    return AdditionalBeanBuildItem.builder()
        .addBeanClasses(
            RatchetStartupTrigger.class,
            QuarkusPrincipalSource.class,
            QuarkusRatchetExecutorProvider.class,
            RatchetRuntimeProducers.class)
        .setUnremovable()
        .build();
  }

  /**
   * Keep all {@code run.ratchet.*} beans. The engine resolves many beans reflectively / by-name via
   * its {@code BeanResolver} ({@code Instance<Object>.select(Class)}) and optional-capability
   * probes, which ArC's removal pass cannot see — without this they would be pruned and overrides
   * would silently vanish. Targeted replacement for {@code quarkus.arc.remove-unused-beans=false}.
   */
  @BuildStep
  UnremovableBeanBuildItem keepRatchetBeans() {
    return new UnremovableBeanBuildItem(
        (BeanInfo bean) ->
            bean.getBeanClass() != null
                && bean.getBeanClass().toString().startsWith("run.ratchet"));
  }

  /**
   * Make ArC discover the engine/store beans and API types from the (external) Ratchet jars.
   * {@code ratchet-api} must be here too: it holds the interceptor-binding annotations (e.g.
   * {@code @CircuitBreakerProtected}) and ships no beans.xml, so it is not a bean archive. Without
   * it in the index ArC discovers {@code CircuitBreakerInterceptor} (from the indexed {@code
   * ratchet} jar) but cannot resolve its binding and fails with "Interceptor has no bindings".
   */
  @BuildStep
  void indexRatchetJars(BuildProducer<IndexDependencyBuildItem> index) {
    index.produce(new IndexDependencyBuildItem("run.ratchet", "ratchet-api"));
    index.produce(new IndexDependencyBuildItem("run.ratchet", "ratchet"));
    index.produce(new IndexDependencyBuildItem("run.ratchet", "ratchet-store-core"));
    index.produce(new IndexDependencyBuildItem("run.ratchet", "ratchet-encryption"));
  }

  /**
   * Defer the engine's {@code @Initialized(ApplicationScoped.class)} auto-start, which fires during
   * STATIC_INIT — too early on Quarkus. {@link RatchetStartupTrigger} drives start from
   * StartupEvent instead. The runtime property covers JVM and native runtime; the native-image
   * property covers the build's STATIC_INIT, which executes inside the image builder JVM.
   */
  @BuildStep
  void deferAutoStart(
      BuildProducer<SystemPropertyBuildItem> runtimeProps,
      BuildProducer<NativeImageSystemPropertyBuildItem> nativeBuildProps) {
    runtimeProps.produce(new SystemPropertyBuildItem(RatchetRuntimeStart.DEFER_PROPERTY, "true"));
    nativeBuildProps.produce(
        new NativeImageSystemPropertyBuildItem(RatchetRuntimeStart.DEFER_PROPERTY, "true"));
  }

  /** Reflection + runtime-init metadata the engine needs in a native image. */
  @BuildStep
  void nativeMetadata(
      BuildProducer<ReflectiveClassBuildItem> reflective,
      BuildProducer<RuntimeInitializedClassBuildItem> runtimeInit) {
    // JobPayload (a record) is reconstructed by JSON-B when a claimed job's payload is read back.
    reflective.produce(
        ReflectiveClassBuildItem.builder("run.ratchet.store.entity.JobPayload")
            .constructors(true)
            .methods(true)
            .fields(true)
            .build());
    // StandaloneExecutorProvider reflectively looks up Executors.newVirtualThreadPerTaskExecutor()
    // to stay compilable at --release 17; register it so ExecutorTargets.VIRTUAL works natively.
    reflective.produce(
        ReflectiveClassBuildItem.builder("java.util.concurrent.Executors").methods(true).build());
    // Static SecureRandom must be created at image runtime, not captured into the image heap.
    runtimeInit.produce(new RuntimeInitializedClassBuildItem("run.ratchet.store.id.UuidV7Factory"));
  }

  /**
   * Register lambda-capturing classes for serialization so method-reference job submission works in
   * native (the lambda's {@code writeReplace} -> {@code SerializedLambda} path). Jandex cannot see
   * call sites, so the heuristic is: any class that injects {@link
   * run.ratchet.api.JobSchedulerService} is a place jobs are submitted.
   */
  @BuildStep
  void lambdaCapturingTypes(
      CombinedIndexBuildItem index, BuildProducer<LambdaCapturingTypeBuildItem> lambdas) {
    for (ClassInfo candidate : index.getIndex().getKnownClasses()) {
      if (injectsJobScheduler(candidate)) {
        lambdas.produce(new LambdaCapturingTypeBuildItem(candidate.name().toString()));
      }
    }
  }

  private static boolean injectsJobScheduler(ClassInfo candidate) {
    for (FieldInfo field : candidate.fields()) {
      if (field.type().name().equals(JOB_SCHEDULER_SERVICE)) {
        return true;
      }
    }
    for (MethodInfo method : candidate.methods()) {
      for (Type parameter : method.parameterTypes()) {
        if (parameter.name().equals(JOB_SCHEDULER_SERVICE)) {
          return true;
        }
      }
    }
    return false;
  }
}
