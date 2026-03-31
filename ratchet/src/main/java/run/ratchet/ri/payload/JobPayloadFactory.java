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
import org.objectweb.asm.Type;

/**
 * A factory class for creating {@link JobPayload} instances from lambda expressions.
 *
 * <p>This utility transforms Java lambda expressions into structured payloads that can be
 * serialized and executed later. It leverages bytecode analysis to extract the target method,
 * arguments, and invocation context from lambda expressions, enabling elegant job scheduling
 * syntax.
 *
 * <h3>Key Features</h3>
 *
 * <ul>
 *   <li><b>Lambda Analysis</b> - Extracts method invocation details from lambda bytecode
 *   <li><b>Type Safety</b> - Preserves type information through method descriptors
 *   <li><b>Argument Capture</b> - Handles captured variables from lambda closures
 *   <li><b>Thread Safety</b> - All methods are stateless and thread-safe
 * </ul>
 *
 * <h3>Supported Lambda Types</h3>
 *
 * <ul>
 *   <li><b>Method References</b> - {@code service::processData}
 *   <li><b>Static Method Calls</b> - {@code () -> EmailService.sendBulkEmails()}
 *   <li><b>Instance Method Calls</b> - {@code () -> processor.handleBatch(items)}
 *   <li><b>Constructor Calls</b> - {@code () -> new ReportGenerator().generate()}
 * </ul>
 *
 * <h3>Limitations</h3>
 *
 * <ul>
 *   <li>Lambda must be {@link Serializable}
 *   <li>Must contain exactly one method invocation
 *   <li>Complex control flow is not supported
 *   <li>Lambda body should be a single expression
 * </ul>
 *
 * @see JobPayload
 * @see AsmLambdaAnalyzer
 */
public final class JobPayloadFactory {

  /**
   * A pre-constructed no-operation payload for special cases where a placeholder job is needed.
   *
   * @see #noop()
   */
  private static final JobPayload NOOP =
      new JobPayload(
          "run.ratchet.ri.util.JobPlaceholders", "noop", "()V", true, List.of());

  /** Private constructor to prevent instantiation of this utility class. */
  private JobPayloadFactory() {}

  /**
   * Creates a JobPayload from a lambda expression.
   *
   * <p>This is a convenience method that delegates to {@link #fromLambda(Serializable, boolean)}
   * with versioning disabled.
   *
   * @param lambda the lambda expression to convert (must be serializable)
   * @return a JobPayload representing the lambda's method invocation
   * @throws NullPointerException if lambda is null
   * @throws IllegalArgumentException if lambda doesn't contain exactly one method call
   */
  public static JobPayload fromLambda(Serializable lambda) {
    return fromLambda(lambda, false);
  }

  /**
   * Creates a JobPayload from a lambda expression while binding runtime arguments.
   *
   * @param lambda the lambda expression to convert (must be serializable)
   * @param runtimeArgs arguments to bind to unresolved invocation parameter slots
   * @return a JobPayload representing the bound invocation
   */
  public static JobPayload fromLambda(Serializable lambda, List<Object> runtimeArgs) {
    Objects.requireNonNull(lambda, "Lambda must not be null");
    Objects.requireNonNull(runtimeArgs, "Runtime args must not be null");

    SerializedLambda sl = toSerializedLambda(lambda);
    JobInvocation joi = AsmLambdaAnalyzer.inspect(sl);

    if (joi.steps().size() != 1) {
      throw new IllegalArgumentException(
          "Job scheduler requires exactly one method invocation (method reference or single method"
              + " call). Found "
              + joi.steps().size()
              + " invocations in lambda: "
              + lambda
              + ". "
              + "\n\nFor complex multi-step logic, create a dedicated method in a CDI bean and"
              + " reference it: "
              + "\n  scheduler.enqueue(() -> myService.processComplexJob(args)).submit(); "
              + "\n\nSee SerializableCheckedRunnable JavaDoc for examples and workarounds.");
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

  /**
   * Creates a JobPayload from a lambda expression with optional versioning.
   *
   * @param lambda the lambda expression to convert (must be serializable)
   * @param versioned whether to create a versioned payload (currently unused)
   * @return a JobPayload representing the lambda's method invocation
   * @throws NullPointerException if lambda is null
   * @throws IllegalArgumentException if lambda doesn't contain exactly one method call
   * @throws IllegalStateException if lambda cannot be serialized
   */
  @SuppressWarnings("java:S1172")
  // versioned parameter reserved for future payload versioning support
  public static JobPayload fromLambda(Serializable lambda, boolean versioned) {
    Objects.requireNonNull(lambda, "Lambda must not be null");

    SerializedLambda sl = toSerializedLambda(lambda);
    JobInvocation joi = AsmLambdaAnalyzer.inspect(sl);

    if (joi.steps().size() != 1) {
      throw new IllegalArgumentException(
          "Job scheduler requires exactly one method invocation (method reference or single method"
              + " call). Found "
              + joi.steps().size()
              + " invocations in lambda: "
              + lambda
              + ". "
              + "\n\nFor complex multi-step logic, create a dedicated method in a CDI bean and"
              + " reference it: "
              + "\n  scheduler.enqueue(() -> myService.processComplexJob(args)).submit(); "
              + "\n\nSee SerializableCheckedRunnable JavaDoc for examples and workarounds.");
    }

    InvocationStep step = resolveNestedFunctionalInvocation(joi.last());
    rejectNonPublicMethod(step);

    // Always return basic payload (versioned payloads are no longer used)
    return new JobPayload(
        internalNameToFqcn(step.ownerInternalName()),
        step.methodName(),
        step.methodDescriptor(),
        step.isStatic(),
        step.arguments());
  }

  /* ─────────────────────────── helper methods ─────────────────────────── */

  /**
   * Returns a no-operation job payload.
   *
   * @return a pre-constructed no-op JobPayload
   */
  public static JobPayload noop() {
    return NOOP;
  }

  /**
   * Converts JVM internal class name format to fully qualified class name.
   *
   * @param internal the internal class name with '/' separators
   * @return the fully qualified class name with '.' separators
   */
  private static String internalNameToFqcn(String internal) {
    return internal.replace('/', '.');
  }

  /**
   * Rejects a lambda that targets a non-public method, failing immediately at payload creation
   * rather than deferring to runtime execution where the error message is less actionable.
   *
   * @param step the resolved invocation step extracted from the lambda
   * @throws IllegalArgumentException if the target method is not public
   */
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
      // Class not loadable here -- downstream validators will catch this
    }
  }

  /**
   * Resolves wrapper invocations that target serializable functional interfaces to the underlying
   * concrete invocation.
   *
   * @param initialStep the invocation extracted from the submitted lambda
   * @return the unwrapped invocation if resolvable, otherwise the original step
   */
  private static InvocationStep resolveNestedFunctionalInvocation(InvocationStep initialStep) {
    InvocationStep resolved = initialStep;

    for (int i = 0; i < 4; i++) {
      InvocationStep next = unwrapFunctionalAdapterInvocation(resolved);
      if (next == null || next.equals(resolved)) {
        return resolved;
      }
      resolved = next;
    }

    return resolved;
  }

  /**
   * Attempts to unwrap a single functional-interface adapter invocation.
   *
   * @param step the invocation step to inspect
   * @return unwrapped invocation step, or {@code null} if unwrapping is not applicable
   */
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

  /**
   * Merges wrapper-call arguments with the nested invocation arguments.
   *
   * @param nestedStep invocation extracted from the captured lambda receiver
   * @param wrapperArgs arguments observed at the wrapper invocation site
   * @return merged argument list for final payload creation
   */
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

  /**
   * Checks whether the invocation targets the single abstract method of a serializable functional
   * interface.
   *
   * @param step invocation step to inspect
   * @return {@code true} when this is a serializable functional-interface adapter invocation
   */
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

  /**
   * Attempts to extract a SerializedLambda from a serializable object.
   *
   * @param value a potentially lambda-backed serializable object
   * @return serialized lambda metadata, or {@code null} if not a serializable lambda
   */
  private static SerializedLambda tryToSerializedLambda(Serializable value) {
    try {
      return toSerializedLambda(value);
    } catch (IllegalStateException e) {
      return null;
    }
  }

  /**
   * Extracts the SerializedLambda representation from a lambda expression.
   *
   * @param lambda the serializable lambda expression
   * @return the SerializedLambda containing lambda metadata
   * @throws IllegalStateException if the lambda cannot be serialized
   */
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
