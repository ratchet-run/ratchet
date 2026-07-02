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
package run.ratchet.ri.payload;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.jboss.logging.Logger;
import org.objectweb.asm.Type;
import run.ratchet.ri.payload.AsmLambdaAnalyzer.InspectionResult;
import run.ratchet.ri.payload.AsmLambdaAnalyzer.InvocationStep;
import run.ratchet.spi.JobInvocation;
import run.ratchet.store.entity.JobPayload;

/**
 * Creates {@link JobPayload} instances from lambda expressions by analyzing their bytecode.
 * Supports method references, static/instance calls, and captured variables. Lambdas must be {@link
 * Serializable} and contain exactly one method invocation.
 *
 * <p>Class names read here come from JVM {@link SerializedLambda} metadata for caller-local
 * lambdas. This factory only extracts the invocation shape; {@code JobSecurityValidator} applies
 * the configured class policy before jobs are persisted or executed.
 *
 * @see JobPayload
 * @see AsmLambdaAnalyzer
 */
public final class JobPayloadFactory {

  private static final Logger log = Logger.getLogger(JobPayloadFactory.class);

  /**
   * Fully-qualified name of the framework's coordination-only placeholder target ({@code
   * run.ratchet.ri.util.JobPlaceholders}). Payloads pointing here are constructed internally (e.g.
   * batch-parent rows), never invoke user code, and so are not subject to the application {@code
   * ClassPolicy}.
   */
  static final String COORDINATION_PLACEHOLDER_TARGET = "run.ratchet.ri.util.JobPlaceholders";

  /**
   * Fully-qualified name of the framework's recurring dispatch shim ({@code
   * run.ratchet.ri.cdi.RecurringMethodInvoker}). Kept as a string so this payload package does not
   * depend on the CDI package. Payloads pointing here are framework-created by {@code
   * RecurringJobProcessor}; {@code RecurringMethodInvoker.invoke()} performs its own {@code
   * ClassPolicy} check against the real annotated bean class before dispatching.
   */
  static final String RECURRING_DISPATCH_TARGET = "run.ratchet.ri.cdi.RecurringMethodInvoker";

  private static final JobPayload NOOP =
      new JobPayload(COORDINATION_PLACEHOLDER_TARGET, "noop", "()V", true, List.of());

  private static final int REFLECTION_CACHE_MAX_ENTRIES = 512;

  private static final Map<MethodLookupKey, VisibilityVerdict> VISIBILITY_CACHE =
      boundedReflectionCache();

  private static final Map<MethodLookupKey, Boolean> FUNCTIONAL_INTERFACE_METHOD_CACHE =
      boundedReflectionCache();

  /**
   * Maximum depth for unwrapping nested functional-interface adapter lambdas (e.g. a {@code
   * SerializableFunction} composed with another). Chosen empirically to cover all practical
   * composition depths we have observed while bounding worst-case analysis cost; pathological
   * deeper chains resolve to the outermost reachable invocation.
   */
  private static final int MAX_FUNCTIONAL_ADAPTER_UNWRAP_DEPTH = 4;

  private JobPayloadFactory() {}

  /**
   * Drops cached reflection results. The cache keys hold strong references to {@link ClassLoader}
   * instances; in redeployable EE containers (WildFly, Payara) the old loader and its classes
   * remain reachable across hot redeploys until evicted by LRU, preventing GC of the previous
   * deployment. Called from the scheduler shutdown path alongside {@code JobTask.clearCaches()}.
   */
  public static void clearCaches() {
    VISIBILITY_CACHE.clear();
    FUNCTIONAL_INTERFACE_METHOD_CACHE.clear();
  }

  /** Creates a {@link JobPayload} from a lambda. */
  public static JobPayload fromLambda(Serializable lambda) {
    return fromInvocation(toInvocation(lambda));
  }

  /**
   * Creates a {@link JobPayload} from a lambda, binding {@code runtimeArgs} to unresolved parameter
   * slots.
   */
  public static JobPayload fromLambda(Serializable lambda, List<Object> runtimeArgs) {
    return fromInvocation(toInvocation(lambda, runtimeArgs));
  }

  public static JobInvocation toInvocation(Serializable lambda, List<Object> runtimeArgs) {
    Objects.requireNonNull(lambda, "Lambda must not be null");
    Objects.requireNonNull(runtimeArgs, "Runtime args must not be null");
    return toInvocationInternal(lambda, runtimeArgs);
  }

  public static JobInvocation toInvocation(Serializable lambda) {
    Objects.requireNonNull(lambda, "Lambda must not be null");
    return toInvocationInternal(lambda, List.of());
  }

  private static JobInvocation toInvocationInternal(Serializable lambda, List<Object> runtimeArgs) {
    SerializedLambda sl = toSerializedLambda(lambda);
    InspectionResult inspection = AsmLambdaAnalyzer.inspect(sl);

    if (inspection.steps().size() != 1) {
      throw new IllegalArgumentException(singleInvocationError(lambda, inspection.steps().size()));
    }

    InvocationStep step = resolveNestedFunctionalInvocation(inspection.last());
    rejectNonPublicMethod(step);
    List<Object> args = mergeInvocationArguments(step, runtimeArgs);

    return new JobInvocation(
        internalNameToFqcn(step.ownerInternalName()),
        step.methodName(),
        step.methodDescriptor(),
        step.isStatic(),
        args);
  }

  public static JobPayload fromInvocation(JobInvocation invocation) {
    Objects.requireNonNull(invocation, "Invocation must not be null");
    return new JobPayload(
        invocation.targetClass(),
        invocation.methodName(),
        invocation.methodDescriptor(),
        invocation.staticMethod(),
        invocation.arguments());
  }

  public static JobPayload noop() {
    return NOOP;
  }

  /**
   * Returns true if {@code payload} targets the framework's internal coordination placeholder. Such
   * payloads — the batch-parent {@link #noop()} — are framework-constructed and never invoke user
   * code, so the persist-time {@code ClassPolicy} gate does not apply to them.
   */
  public static boolean isCoordinationPlaceholder(JobPayload payload) {
    return payload != null && COORDINATION_PLACEHOLDER_TARGET.equals(payload.target());
  }

  /**
   * Returns true if {@code payload} targets the framework's internal recurring dispatch shim. The
   * shim is allowed through framework class-policy gates because it never decides the user target
   * itself: {@code RecurringMethodInvoker.invoke()} validates the real annotated bean class against
   * the configured {@code ClassPolicy} before invoking it.
   */
  public static boolean isRecurringDispatchShim(JobPayload payload) {
    return payload != null && isRecurringDispatchShim(payload.target());
  }

  /** String overload for call sites that only have the target class name. */
  public static boolean isRecurringDispatchShim(String className) {
    return RECURRING_DISPATCH_TARGET.equals(className);
  }

  private static String singleInvocationError(Serializable lambda, int stepCount) {
    return "Job scheduler requires exactly one method invocation (method reference or single method"
        + " call). Found "
        + stepCount
        + " invocations in lambda: "
        + lambda
        + ". "
        + "\n\nFor complex multi-step logic, create a dedicated method in a CDI bean and"
        + " reference it: "
        + "\n  scheduler.enqueue(() -> myService.processComplexJob(args)).submit(); "
        + "\n\nSee SerializableCheckedRunnable JavaDoc for examples and workarounds.";
  }

  private static String internalNameToFqcn(String internal) {
    return internal.replace('/', '.');
  }

  private static void rejectNonPublicMethod(InvocationStep step) {
    String className = internalNameToFqcn(step.ownerInternalName());
    MethodLookupKey key =
        new MethodLookupKey(
            className,
            step.methodName(),
            step.methodDescriptor(),
            Thread.currentThread().getContextClassLoader());
    VisibilityVerdict verdict = cached(VISIBILITY_CACHE, key, JobPayloadFactory::resolveVisibility);

    if (!verdict.publicOrUnknown()) {
      throw new IllegalArgumentException(
          "Cannot schedule "
              + verdict.visibility()
              + " method '"
              + step.methodName()
              + "' in "
              + className
              + ". "
              + "The job scheduler invokes methods via reflection, so only public methods are "
              + "supported. Change the method visibility to public.");
    }
  }

  private static VisibilityVerdict resolveVisibility(MethodLookupKey key) {
    try {
      Class<?> clazz = Class.forName(key.className(), false, key.classLoader());

      Method matched =
          Arrays.stream(clazz.getDeclaredMethods())
              .filter(
                  m ->
                      m.getName().equals(key.methodName())
                          && Type.getMethodDescriptor(m).equals(key.methodDescriptor()))
              .findFirst()
              .orElse(null);

      if (matched == null || Modifier.isPublic(matched.getModifiers())) {
        return VisibilityVerdict.PUBLIC_OR_UNKNOWN;
      }

      String visibility =
          Modifier.isPrivate(matched.getModifiers())
              ? "private"
              : Modifier.isProtected(matched.getModifiers()) ? "protected" : "package-private";
      return new VisibilityVerdict(false, visibility);
    } catch (ClassNotFoundException e) {
      // Target class cannot be loaded from this context — we cannot verify visibility, but we do
      // not want to block scheduling on a reflective lookup miss. The invocation path will surface
      // a clearer error at execution time if the class is genuinely missing.
      log.debugf(e, "Cannot load %s for visibility check; skipping", key.className());
      return VisibilityVerdict.PUBLIC_OR_UNKNOWN;
    }
  }

  private static InvocationStep resolveNestedFunctionalInvocation(InvocationStep initialStep) {
    InvocationStep resolved = initialStep;

    for (int i = 0; i < MAX_FUNCTIONAL_ADAPTER_UNWRAP_DEPTH; i++) {
      InvocationStep next = unwrapFunctionalAdapterInvocation(resolved);
      if (next == null || next.equals(resolved)) {
        return resolved;
      }
      resolved = next;
    }

    return resolved;
  }

  private static InvocationStep unwrapFunctionalAdapterInvocation(InvocationStep step) {
    if (step.isStatic() || !isSerializableFunctionalInterfaceMethod(step)) {
      return null;
    }
    if (!(step.receiver() instanceof Serializable receiverLambda)) {
      return null;
    }

    SerializedLambda nestedSerializedLambda = tryToSerializedLambda(receiverLambda);
    if (nestedSerializedLambda == null) {
      return null;
    }

    InspectionResult nestedInvocation = AsmLambdaAnalyzer.inspect(nestedSerializedLambda);
    if (nestedInvocation.steps().size() != 1) {
      return null;
    }

    InvocationStep nestedStep = nestedInvocation.last();
    List<Object> mergedArguments = mergeInvocationArguments(nestedStep, step.arguments());

    return new InvocationStep(
        nestedStep.ownerInternalName(),
        nestedStep.methodName(),
        nestedStep.methodDescriptor(),
        nestedStep.isStatic(),
        mergedArguments,
        nestedStep.receiver());
  }

  private static List<Object> mergeInvocationArguments(
      InvocationStep nestedStep, List<Object> wrapperArgs) {
    /*
     * Functional adapter unwrapping merges wrapper-supplied values into unresolved null slots from
     * the nested invocation. If ASM already resolved every nested parameter but the wrapper captured
     * the complete target argument list, the arity fallback below keeps those wrapper values instead
     * of dropping them as leftovers.
     */
    int nestedParamCount = Type.getArgumentTypes(nestedStep.methodDescriptor()).length;

    // Target method takes no arguments — wrapperArgs are loop variables or other
    // values the lambda never forwards, so drop them rather than leak into the payload.
    if (nestedParamCount == 0) {
      return List.of();
    }

    if (wrapperArgs.isEmpty()) {
      return nestedStep.arguments();
    }

    List<Object> nestedArgs = nestedStep.arguments();
    if (nestedArgs.isEmpty()) {
      return List.copyOf(wrapperArgs);
    }

    List<Object> merged = new ArrayList<>(nestedArgs);
    int wrapperIndex = 0;
    for (int i = 0; i < merged.size() && wrapperIndex < wrapperArgs.size(); i++) {
      if (merged.get(i) == null) {
        merged.set(i, wrapperArgs.get(wrapperIndex++));
      }
    }

    if (wrapperIndex == wrapperArgs.size()) {
      return List.copyOf(merged);
    }

    if (wrapperArgs.size() == nestedParamCount) {
      return List.copyOf(wrapperArgs);
    }

    return List.copyOf(merged);
  }

  private static boolean isSerializableFunctionalInterfaceMethod(InvocationStep step) {
    String ownerClassName = internalNameToFqcn(step.ownerInternalName());
    MethodLookupKey key =
        new MethodLookupKey(
            ownerClassName,
            step.methodName(),
            step.methodDescriptor(),
            Thread.currentThread().getContextClassLoader());

    return cached(
        FUNCTIONAL_INTERFACE_METHOD_CACHE,
        key,
        JobPayloadFactory::resolveSerializableFunctionalInterfaceMethod);
  }

  private static boolean resolveSerializableFunctionalInterfaceMethod(MethodLookupKey key) {
    try {
      Class<?> ownerClass = Class.forName(key.className(), false, key.classLoader());

      if (!ownerClass.isInterface() || !Serializable.class.isAssignableFrom(ownerClass)) {
        return false;
      }

      int matchingAbstractMethods = 0;
      for (Method method : ownerClass.getMethods()) {
        if (method.getDeclaringClass() == Object.class
            || !Modifier.isAbstract(method.getModifiers())) {
          continue;
        }
        if (method.getName().equals(key.methodName())
            && Type.getMethodDescriptor(method).equals(key.methodDescriptor())) {
          matchingAbstractMethods++;
        }
      }

      return matchingAbstractMethods == 1;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private record MethodLookupKey(
      String className, String methodName, String methodDescriptor, ClassLoader classLoader) {}

  private record VisibilityVerdict(boolean publicOrUnknown, String visibility) {
    private static final VisibilityVerdict PUBLIC_OR_UNKNOWN = new VisibilityVerdict(true, null);
  }

  private static <V> Map<MethodLookupKey, V> boundedReflectionCache() {
    return Collections.synchronizedMap(
        new LinkedHashMap<>(64, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<MethodLookupKey, V> eldest) {
            return size() > REFLECTION_CACHE_MAX_ENTRIES;
          }
        });
  }

  // All callers pass either VISIBILITY_CACHE or FUNCTIONAL_INTERFACE_METHOD_CACHE — both static
  // final fields — so the parameter holds a stable identity. External sync is required because
  // the LinkedHashMap LRU promotion in get() makes get-then-put non-atomic on the underlying
  // synchronizedMap.
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private static <V> V cached(
      Map<MethodLookupKey, V> cache, MethodLookupKey key, Function<MethodLookupKey, V> resolver) {
    synchronized (cache) {
      V cached = cache.get(key);
      if (cached != null) {
        return cached;
      }
      V resolved = resolver.apply(key);
      cache.put(key, resolved);
      return resolved;
    }
  }

  private static SerializedLambda tryToSerializedLambda(Serializable value) {
    try {
      return toSerializedLambda(value);
    } catch (IllegalStateException e) {
      log.debugf(
          e,
          "Failed to unwrap nested lambda receiver %s; treating it as a regular receiver",
          value.getClass().getName());
      return null;
    }
  }

  private static SerializedLambda toSerializedLambda(Serializable lambda) {
    return LambdaSerialization.toSerializedLambda(
        lambda, "Unable to serialise lambda -- did you forget to make it Serializable?");
  }
}
