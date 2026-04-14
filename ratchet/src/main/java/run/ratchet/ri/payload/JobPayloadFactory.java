package run.ratchet.ri.payload;

import run.ratchet.ri.payload.AsmLambdaAnalyzer.InvocationStep;
import run.ratchet.ri.payload.AsmLambdaAnalyzer.JobInvocation;
import run.ratchet.store.entity.JobPayload;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.jboss.logging.Logger;
import org.objectweb.asm.Type;

/**
 * Creates {@link JobPayload} instances from lambda expressions by analyzing their bytecode.
 * Supports method references, static/instance calls, and captured variables. Lambdas must be {@link
 * Serializable} and contain exactly one method invocation.
 *
 * @see JobPayload
 * @see AsmLambdaAnalyzer
 */
public final class JobPayloadFactory {

  private static final Logger log = Logger.getLogger(JobPayloadFactory.class);

  private static final JobPayload NOOP =
      new JobPayload(
          "run.ratchet.ri.util.JobPlaceholders", "noop", "()V", true, List.of());

  /**
   * Maximum depth for unwrapping nested functional-interface adapter lambdas (e.g. a {@code
   * SerializableFunction} composed with another). Chosen empirically to cover all practical
   * composition depths we have observed while bounding worst-case analysis cost; pathological
   * deeper chains resolve to the outermost reachable invocation.
   */
  private static final int MAX_FUNCTIONAL_ADAPTER_UNWRAP_DEPTH = 4;

  private JobPayloadFactory() {}

  /**
   * Creates a {@link JobPayload} from a lambda; delegates to {@link #fromLambda(Serializable,
   * boolean)} with versioning disabled.
   */
  public static JobPayload fromLambda(Serializable lambda) {
    return fromLambda(lambda, false);
  }

  /**
   * Creates a {@link JobPayload} from a lambda, binding {@code runtimeArgs} to unresolved parameter
   * slots.
   */
  public static JobPayload fromLambda(Serializable lambda, List<Object> runtimeArgs) {
    Objects.requireNonNull(lambda, "Lambda must not be null");
    Objects.requireNonNull(runtimeArgs, "Runtime args must not be null");

    SerializedLambda sl = toSerializedLambda(lambda);
    JobInvocation joi = AsmLambdaAnalyzer.inspect(sl);

    if (joi.steps().size() != 1) {
      throw new IllegalArgumentException(singleInvocationError(lambda, joi.steps().size()));
    }

    InvocationStep step = resolveNestedFunctionalInvocation(joi.last());
    rejectNonPublicMethod(step);
    List<Object> args = mergeInvocationArguments(step, runtimeArgs);

    return new JobPayload(
        internalNameToFqcn(step.ownerInternalName()),
        step.methodName(),
        step.methodDescriptor(),
        step.isStatic(),
        args);
  }

  @SuppressWarnings("java:S1172")
  // versioned parameter reserved for future payload versioning support
  public static JobPayload fromLambda(Serializable lambda, boolean versioned) {
    Objects.requireNonNull(lambda, "Lambda must not be null");

    SerializedLambda sl = toSerializedLambda(lambda);
    JobInvocation joi = AsmLambdaAnalyzer.inspect(sl);

    if (joi.steps().size() != 1) {
      throw new IllegalArgumentException(singleInvocationError(lambda, joi.steps().size()));
    }

    InvocationStep step = resolveNestedFunctionalInvocation(joi.last());
    rejectNonPublicMethod(step);

    return new JobPayload(
        internalNameToFqcn(step.ownerInternalName()),
        step.methodName(),
        step.methodDescriptor(),
        step.isStatic(),
        step.arguments());
  }

  public static JobPayload noop() {
    return NOOP;
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
    try {
      Class<?> clazz =
          Class.forName(className, false, Thread.currentThread().getContextClassLoader());

      Method matched =
          Arrays.stream(clazz.getDeclaredMethods())
              .filter(
                  m ->
                      m.getName().equals(step.methodName())
                          && Type.getMethodDescriptor(m).equals(step.methodDescriptor()))
              .findFirst()
              .orElse(null);

      if (matched != null && !Modifier.isPublic(matched.getModifiers())) {
        String visibility =
            Modifier.isPrivate(matched.getModifiers())
                ? "private"
                : Modifier.isProtected(matched.getModifiers()) ? "protected" : "package-private";
        throw new IllegalArgumentException(
            "Cannot schedule "
                + visibility
                + " method '"
                + step.methodName()
                + "' in "
                + className
                + ". "
                + "The job scheduler invokes methods via reflection, so only public methods are "
                + "supported. Change the method visibility to public.");
      }
    } catch (ClassNotFoundException e) {
      // Target class cannot be loaded from this context — we cannot verify visibility, but we do
      // not want to block scheduling on a reflective lookup miss. The invocation path will surface
      // a clearer error at execution time if the class is genuinely missing.
      log.debugf(e, "Cannot load %s for visibility check; skipping", className);
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

    JobInvocation nestedInvocation = AsmLambdaAnalyzer.inspect(nestedSerializedLambda);
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

    int nestedParamCount = Type.getArgumentTypes(nestedStep.methodDescriptor()).length;
    if (wrapperArgs.size() == nestedParamCount) {
      return List.copyOf(wrapperArgs);
    }

    return List.copyOf(merged);
  }

  private static boolean isSerializableFunctionalInterfaceMethod(InvocationStep step) {
    String ownerClassName = internalNameToFqcn(step.ownerInternalName());

    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      Class<?> ownerClass = Class.forName(ownerClassName, false, classLoader);

      if (!ownerClass.isInterface() || !Serializable.class.isAssignableFrom(ownerClass)) {
        return false;
      }

      int matchingAbstractMethods = 0;
      for (Method method : ownerClass.getMethods()) {
        if (method.getDeclaringClass() == Object.class
            || !Modifier.isAbstract(method.getModifiers())) {
          continue;
        }
        if (method.getName().equals(step.methodName())
            && Type.getMethodDescriptor(method).equals(step.methodDescriptor())) {
          matchingAbstractMethods++;
        }
      }

      return matchingAbstractMethods == 1;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private static SerializedLambda tryToSerializedLambda(Serializable value) {
    try {
      return toSerializedLambda(value);
    } catch (IllegalStateException e) {
      return null;
    }
  }

  @SuppressWarnings("java:S3011")
  // setAccessible is required for lambda serialization - accessing compiler-generated writeReplace
  private static SerializedLambda toSerializedLambda(Serializable lambda) {
    try {
      Method m = lambda.getClass().getDeclaredMethod("writeReplace");
      m.setAccessible(true);
      return (SerializedLambda) m.invoke(lambda);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Unable to serialise lambda -- did you forget to make it Serializable?", e);
    }
  }
}
