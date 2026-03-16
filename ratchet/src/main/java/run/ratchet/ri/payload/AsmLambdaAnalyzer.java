package run.ratchet.ri.payload;

import run.ratchet.spi.LambdaAnalyzer;
import run.ratchet.spi.LambdaDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.function.IntBinaryOperator;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

/**
 *
 *
 * <h2>AsmLambdaAnalyzer</h2>
 *
 * <p>A sophisticated bytecode analyzer that inspects Java lambda expressions to extract method
 * invocation details. This class goes beyond simple method references to understand
 * <em>complex</em> lambda expressions by analyzing their bytecode structure. It walks through the
 * synthetic <code>lambda$...</code> implementation method using ASM's tree API, performs a
 * lightweight operand-stack simulation, and records <b>every</b> method invocation it encounters,
 * including:
 *
 * <ul>
 *   <li>The owner class (internal name)
 *   <li>Method name
 *   <li>Method descriptor (parameter and return types)
 *   <li>Concrete argument objects to be supplied at runtime
 * </ul>
 *
 * <p>
 *
 * <h3>Supported Operations</h3>
 *
 * <p>This analyzer is <strong>not</strong> a full JVM interpreter. It supports a targeted subset of
 * bytecode operations commonly produced by the Java compiler (javac) for idiomatic lambdas used in
 * background jobs:
 *
 * <ul>
 *   <li>Constant loading (null, integers, floats, doubles, strings, etc.)
 *   <li>Captured variable loading from the enclosing scope
 *   <li>Basic arithmetic operations and string concatenation
 *   <li>Object construction via {@code new ...; invokespecial <init>}
 *   <li>Method invocations (static, virtual, interface, and special)
 *   <li>Basic stack manipulation (pop, dup)
 * </ul>
 *
 * <p>Unsupported instructions are handled gracefully by pushing an {@link UnknownValue} marker onto
 * the simulated operand stack, preserving the stack's integrity without silently compromising the
 * analysis.
 *
 * <p>
 *
 * <h3>Usage</h3>
 *
 * <p>The primary client of this class is {@code JobPayloadFactory}, which uses the extracted
 * invocation information to create serializable job payloads. The factory can select which
 * invocation it wants to schedule -- typically the <em>last</em> one in the lambda body. However,
 * by preserving the complete list of invocations, this design enables more sophisticated scenarios
 * such as pre-processing calls or complex execution chains.
 */
public final class AsmLambdaAnalyzer implements LambdaAnalyzer {

  /** Private constructor to prevent external instantiation. */
  public AsmLambdaAnalyzer() {
    // Default constructor for SPI usage
  }

  /* ───────────────────────── LambdaAnalyzer SPI ───────────────────────── */

  /**
   * Analyzes a serialized lambda expression to extract its method invocation details.
   *
   * <p>This is the primary entry point for the lambda inspection process. It accepts a {@link
   * SerializedLambda} instance (typically obtained via serialization of a lambda expression) and
   * performs deep analysis to determine what methods the lambda will invoke when executed.
   *
   * <p>The method handles two distinct types of lambda implementations:
   *
   * <ol>
   *   <li><strong>Method References</strong> - Simple lambdas that directly reference an existing
   *       method (e.g., {@code String::length} or {@code Math::max})
   *   <li><strong>Inline Lambdas</strong> - Complex lambdas with a body containing arbitrary code
   *       (e.g., {@code (x) -> x.process().then().validate()})
   * </ol>
   *
   * @param serializedLambda the serialized representation of a lambda expression to analyze; must
   *     not be null
   * @return a {@link JobInvocation} object containing all method invocations found in the lambda,
   *     in the order they would be executed
   * @throws NullPointerException if the provided SerializedLambda is null
   * @throws IllegalStateException if the lambda contains no invokable methods or if a synthetic
   *     lambda method cannot be found in the bytecode
   * @see JobInvocation
   * @see InvocationStep
   */
  public static JobInvocation inspect(SerializedLambda serializedLambda) {
    Objects.requireNonNull(serializedLambda, "SerializedLambda must not be null");

    // ── 1. Method reference (static or instance) ──
    if (!serializedLambda.getImplMethodName().startsWith("lambda$")) {
      return new JobInvocation(List.of(handleMethodReference(serializedLambda)));
    }

    // ── 2. Inline lambda (i.e. lambda$... method) ──
    return handleInlineLambda(serializedLambda);
  }

  /* ─────────────────────────── Public API ─────────────────────────── */

  /**
   * Simulates a binary arithmetic operation on two integer operands during bytecode analysis.
   *
   * @param operandStack the simulated JVM operand stack
   * @param operation the binary operation to apply
   */
  private static void binaryOpInt(Deque<Value> operandStack, IntBinaryOperator operation) {
    Object rightOperand = resolveValue(operandStack.pop(), new Object[0]);
    Object leftOperand = resolveValue(operandStack.pop(), new Object[0]);
    if (leftOperand instanceof Integer leftInt && rightOperand instanceof Integer rightInt) {
      operandStack.push(new ConstantValue(operation.applyAsInt(leftInt, rightInt)));
    } else {
      operandStack.push(UnknownValue.INSTANCE);
    }
  }

  /**
   * Locates a specific method within a class's bytecode representation.
   *
   * @param classNode the ASM {@link ClassNode} instance
   * @param methodName the name of the method to find
   * @param methodDesc the JVM descriptor of the method to find
   * @return the matching {@link MethodNode}, or null if not found
   */
  private static MethodNode findMethodNode(
      ClassNode classNode, String methodName, String methodDesc) {
    for (MethodNode methodNode : classNode.methods) {
      if (methodNode.name.equals(methodName) && methodNode.desc.equals(methodDesc)) {
        return methodNode;
      }
    }
    return null;
  }

  /**
   * Processes a method invocation bytecode instruction during lambda analysis.
   *
   * @param operandStack the simulated JVM operand stack
   * @param invocationList the list to accumulate invocations
   * @param methodNode the ASM node representing the method invocation instruction
   * @param opcodeValue the JVM opcode of the invocation instruction
   * @param capturedValues captured values from the lambda's enclosing scope
   */
  private static void handleGenericInvoke(
      Deque<Value> operandStack,
      List<InvocationStep> invocationList,
      MethodInsnNode methodNode,
      int opcodeValue,
      Object[] capturedValues) {
    Type[] argumentTypes = Type.getArgumentTypes(methodNode.desc);
    Object[] resolvedArguments = new Object[argumentTypes.length];

    for (int i = argumentTypes.length - 1; i >= 0; i--) {
      resolvedArguments[i] = resolveValue(operandStack.pop(), capturedValues);
    }

    Object resolvedReceiver = null;
    if (opcodeValue != Opcodes.INVOKESTATIC) {
      resolvedReceiver = resolveValue(operandStack.pop(), capturedValues);
    }

    invocationList.add(
        new InvocationStep(
            methodNode.owner,
            methodNode.name,
            methodNode.desc,
            opcodeValue == Opcodes.INVOKESTATIC,
            Collections.unmodifiableList(Arrays.asList(resolvedArguments)),
            resolvedReceiver));

    if (Type.getReturnType(methodNode.desc) != Type.VOID_TYPE) {
      operandStack.push(UnknownValue.INSTANCE);
    }
  }

  /**
   * Analyzes the bytecode of an inline lambda expression to extract its method invocations.
   *
   * @param serializedLambda the serialized representation of an inline lambda expression
   * @return a {@link JobInvocation} containing all method invocations found in the lambda body
   * @throws IllegalStateException if the synthetic lambda method cannot be found or contains no
   *     method invocations
   */
  @SuppressWarnings({"java:S3776", "java:S6541"})
  // Cognitive complexity is inherent to bytecode switch - each case is simple
  private static JobInvocation handleInlineLambda(SerializedLambda serializedLambda) {
    ClassNode lambdaClass = readClassNode(serializedLambda.getImplClass());

    MethodNode lambdaMethod =
        findMethodNode(
            lambdaClass,
            serializedLambda.getImplMethodName(),
            serializedLambda.getImplMethodSignature());

    if (lambdaMethod == null) {
      throw new IllegalStateException(
          "Synthetic lambda method not found: " + serializedLambda.getImplMethodName());
    }

    Object[] capturedValues = new Object[serializedLambda.getCapturedArgCount()];
    for (int i = 0; i < capturedValues.length; i++) {
      capturedValues[i] = serializedLambda.getCapturedArg(i);
    }

    Deque<Value> operandStack = new ArrayDeque<>();
    List<InvocationStep> invocationList = new ArrayList<>();

    for (AbstractInsnNode node : lambdaMethod.instructions) {
      int opcodeValue = node.getOpcode();
      if (opcodeValue < 0) {
        continue;
      }

      switch (opcodeValue) {
        // ---- Constant loading instructions ----
        case Opcodes.ACONST_NULL -> operandStack.push(new ConstantValue(null));
        case Opcodes.ICONST_M1,
            Opcodes.ICONST_0,
            Opcodes.ICONST_1,
            Opcodes.ICONST_2,
            Opcodes.ICONST_3,
            Opcodes.ICONST_4,
            Opcodes.ICONST_5 ->
            operandStack.push(new ConstantValue(opcodeValue - Opcodes.ICONST_0));
        case Opcodes.LCONST_0, Opcodes.LCONST_1 ->
            operandStack.push(new ConstantValue((long) (opcodeValue - Opcodes.LCONST_0)));
        case Opcodes.FCONST_0, Opcodes.FCONST_1, Opcodes.FCONST_2 ->
            operandStack.push(new ConstantValue((float) (opcodeValue - Opcodes.FCONST_0)));
        case Opcodes.DCONST_0, Opcodes.DCONST_1 ->
            operandStack.push(new ConstantValue((double) (opcodeValue - Opcodes.DCONST_0)));
        case Opcodes.BIPUSH, Opcodes.SIPUSH ->
            operandStack.push(new ConstantValue(((IntInsnNode) node).operand));
        case Opcodes.LDC -> operandStack.push(new ConstantValue(((LdcInsnNode) node).cst));

        /* ───────────── captured var loads ───────────── */
        case Opcodes.ALOAD, Opcodes.ILOAD, Opcodes.LLOAD, Opcodes.FLOAD, Opcodes.DLOAD -> {
          int varIndex = ((VarInsnNode) node).var;
          if (varIndex < capturedValues.length) {
            operandStack.push(new CapturedValue(varIndex));
          } else {
            operandStack.push(UnknownValue.INSTANCE);
          }
        }

        /* ───────────── arithmetic operations ───────────── */
        case Opcodes.IADD -> binaryOpInt(operandStack, Integer::sum);
        case Opcodes.ISUB -> binaryOpInt(operandStack, (a, b) -> a - b);
        case Opcodes.IMUL -> binaryOpInt(operandStack, (a, b) -> a * b);
        case Opcodes.IDIV -> binaryOpInt(operandStack, (a, b) -> a / b);
        case Opcodes.IREM -> binaryOpInt(operandStack, (a, b) -> a % b);

        /* ───────────── stack manipulation ───────────── */
        case Opcodes.POP -> operandStack.pop();
        case Opcodes.DUP -> operandStack.push(operandStack.peek());

        /* ───────────── object creation (no-arg ctor only) ───────────── */
        case Opcodes.NEW -> {
          TypeInsnNode typeNode = (TypeInsnNode) node;
          operandStack.push(new NewInstanceMarker(typeNode.desc));
        }
        case Opcodes.INVOKESPECIAL -> {
          MethodInsnNode methodInsn = (MethodInsnNode) node;
          if ("<init>".equals(methodInsn.name)) {
            Value topValue = operandStack.pop();
            if (topValue instanceof NewInstanceMarker marker
                && marker.desc().equals(methodInsn.owner)) {
              operandStack.push(new ConstantValue(instantiateNoArg(marker.desc())));
            } else {
              operandStack.push(UnknownValue.INSTANCE);
            }
          } else {
            handleGenericInvoke(
                operandStack, invocationList, methodInsn, opcodeValue, capturedValues);
          }
        }

        /* ───────────── method invocations ───────────── */
        case Opcodes.INVOKESTATIC, Opcodes.INVOKEVIRTUAL, Opcodes.INVOKEINTERFACE -> {
          MethodInsnNode methodInsn = (MethodInsnNode) node;
          handleGenericInvoke(
              operandStack, invocationList, methodInsn, opcodeValue, capturedValues);
        }

        default -> operandStack.push(UnknownValue.INSTANCE);
      }
    }

    if (invocationList.isEmpty()) {
      throw new IllegalStateException(
          "Lambda body contained no method calls that could be inspected");
    }

    return new JobInvocation(List.copyOf(invocationList));
  }

  /**
   * Processes a method reference lambda to extract its invocation details.
   *
   * @param serializedLambda the serialized representation of a method reference lambda
   * @return an {@link InvocationStep} containing the referenced method details
   */
  private static InvocationStep handleMethodReference(SerializedLambda serializedLambda) {
    List<Object> capturedArguments = new ArrayList<>(serializedLambda.getCapturedArgCount());
    for (int i = 0; i < serializedLambda.getCapturedArgCount(); i++) {
      capturedArguments.add(serializedLambda.getCapturedArg(i));
    }

    boolean isStatic = serializedLambda.getImplMethodKind() == Opcodes.H_INVOKESTATIC;
    Object receiver = null;
    List<Object> invocationArguments = capturedArguments;
    if (!isStatic && !capturedArguments.isEmpty()) {
      receiver = capturedArguments.get(0);
      invocationArguments = capturedArguments.subList(1, capturedArguments.size());
    }

    return new InvocationStep(
        serializedLambda.getImplClass(),
        serializedLambda.getImplMethodName(),
        serializedLambda.getImplMethodSignature(),
        isStatic,
        Collections.unmodifiableList(new ArrayList<>(invocationArguments)),
        receiver);
  }

  /**
   * Dynamically instantiates a class using its no-argument constructor.
   *
   * <p>Only classes outside the JDK and well-known framework packages are eligible for
   * instantiation. This prevents side effects from constructors or static initializers in
   * system-level or security-sensitive classes when analyzing lambda bytecode from untrusted
   * payloads.
   *
   * @param internalClassName the internal JVM name of the class to instantiate
   * @return a new instance of the specified class, or null if instantiation fails or is blocked
   */
  @SuppressWarnings("java:S1181") // Must catch Throwable since reflection can throw various errors
  private static Object instantiateNoArg(String internalClassName) {
    String canonicalClassName = internalClassName.replace('/', '.');
    if (isBlockedClassName(canonicalClassName)) {
      return null;
    }
    try {
      Class<?> classToInstantiate =
          Class.forName(canonicalClassName, false, Thread.currentThread().getContextClassLoader());
      return classToInstantiate.getDeclaredConstructor().newInstance();
    } catch (Throwable instantiationError) {
      return null; // if instantiation fails, treat as unknown
    }
  }

  /**
   * Returns true if the class name should not be instantiated during lambda analysis.
   *
   * <p>Blocks JDK core classes, security-sensitive packages, and known runtime internals to prevent
   * side effects when analyzing lambda bytecode from untrusted payloads.
   */
  private static boolean isBlockedClassName(String canonicalClassName) {
    return canonicalClassName.startsWith("java.")
        || canonicalClassName.startsWith("javax.")
        || canonicalClassName.startsWith("jakarta.")
        || canonicalClassName.startsWith("sun.")
        || canonicalClassName.startsWith("com.sun.")
        || canonicalClassName.startsWith("jdk.")
        || canonicalClassName.startsWith("org.objectweb.asm.");
  }

  /**
   * Loads and parses the bytecode of a class into an ASM tree representation.
   *
   * @param classInternalName the JVM internal name of the class to load
   * @return a fully populated {@link ClassNode} containing the class structure and bytecode
   * @throws IllegalStateException if the class file cannot be found or read
   */
  private static ClassNode readClassNode(String classInternalName) {
    String classFileResource = classInternalName + ".class";

    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    try (InputStream classFileStream = contextClassLoader.getResourceAsStream(classFileResource)) {
      if (classFileStream == null) {
        throw new IllegalStateException("Bytecode not found for " + classInternalName);
      }

      ClassReader bytecodeReader = new ClassReader(classFileStream);
      ClassNode classStructure = new ClassNode();

      bytecodeReader.accept(classStructure, ClassReader.SKIP_FRAMES);

      return classStructure;
    } catch (IOException ioException) {
      throw new IllegalStateException("Unable to read class " + classInternalName, ioException);
    }
  }

  /**
   * Converts an abstract {@link Value} from the simulated operand stack into a concrete Java
   * object.
   *
   * @param abstractValue the abstract {@link Value} from the simulated operand stack to resolve
   * @param capturedValues captured values from the lambda's enclosing scope
   * @return the concrete Java object, or null for unknown value types
   */
  private static Object resolveValue(Value abstractValue, Object[] capturedValues) {
    if (abstractValue instanceof ConstantValue cv) {
      return cv.value();
    } else if (abstractValue instanceof CapturedValue cap) {
      return capturedValues[cap.index()];
    }
    return null;
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

  /**
   * Analyzes a serializable lambda expression to extract its target method information.
   *
   * @param lambda the serializable lambda expression to analyze
   * @return a {@link LambdaDescriptor} describing the lambda's target method
   * @throws NullPointerException if lambda is null
   * @throws IllegalStateException if the lambda cannot be serialized or analyzed
   */
  @Override
  public LambdaDescriptor analyze(Serializable lambda) {
    Objects.requireNonNull(lambda, "Lambda must not be null");
    SerializedLambda sl = toSerializedLambda(lambda);
    JobInvocation joi = inspect(sl);
    InvocationStep step = joi.last();
    return new LambdaDescriptor(
        step.ownerInternalName().replace('/', '.'),
        step.methodName(),
        step.methodDescriptor(),
        step.isStatic(),
        step.arguments().toArray());
  }

  /* ───────────────────────── Inner Types ───────────────────────── */

  private enum UnknownValue implements Value {
    INSTANCE
  }

  private sealed interface Value
      permits ConstantValue, CapturedValue, NewInstanceMarker, UnknownValue {}

  /**
   * Represents an immutable description of a single method invocation site within a lambda
   * expression.
   *
   * @param ownerInternalName the internal name of the class that owns the method
   * @param methodName the name of the method being invoked
   * @param methodDescriptor the method descriptor in JVM format
   * @param isStatic {@code true} if the method is static
   * @param arguments the list of resolved argument objects
   * @param receiver the resolved receiver object for instance invocations, or {@code null}
   */
  public record InvocationStep(
      String ownerInternalName,
      String methodName,
      String methodDescriptor,
      boolean isStatic,
      List<Object> arguments,
      Object receiver) {}

  /**
   * Represents the complete result of a lambda expression inspection.
   *
   * @param steps an ordered, immutable list of invocation steps
   */
  public record JobInvocation(List<InvocationStep> steps) {

    /**
     * Retrieves the last invocation step in the sequence.
     *
     * @return the last {@link InvocationStep}
     * @throws IllegalStateException if the steps list is empty
     */
    public InvocationStep last() {
      if (steps.isEmpty()) {
        throw new IllegalStateException("No invocation found in lambda");
      }
      return steps.get(steps.size() - 1);
    }
  }

  private record ConstantValue(Object value) implements Value {}

  private record CapturedValue(int index) implements Value {}

  private record NewInstanceMarker(String desc) implements Value {}
}
