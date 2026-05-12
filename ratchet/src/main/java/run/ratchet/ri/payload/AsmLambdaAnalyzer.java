package run.ratchet.ri.payload;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
import run.ratchet.spi.LambdaAnalyzer;
import run.ratchet.spi.LambdaDescriptor;

/**
 * Bytecode analyzer that inspects Java lambda expressions to extract method invocation details. For
 * simple method references it reads the {@link SerializedLambda} metadata directly; for inline
 * lambdas it walks the synthetic {@code lambda$...} method using ASM's tree API with a lightweight
 * operand-stack simulation, recording every invocation (owner, method name, descriptor, and
 * resolved arguments).
 *
 * <p>The simulated stack uses a sealed {@link Value} interface ({@link ConstantValue}, {@link
 * CapturedValue}, {@link NewInstanceMarker}, {@link UnknownValue}) so unsupported instructions
 * degrade gracefully without corrupting the analysis.
 */
public final class AsmLambdaAnalyzer implements LambdaAnalyzer {

  public AsmLambdaAnalyzer() {}

  /**
   * Analyzes a {@link SerializedLambda} and returns the method invocations it contains. Simple
   * method references are read directly from metadata; inline lambdas are walked via ASM.
   *
   * @throws IllegalStateException if no method invocations are found or the synthetic method is
   *     missing from bytecode
   */
  static InspectionResult inspect(SerializedLambda serializedLambda) {
    Objects.requireNonNull(serializedLambda, "SerializedLambda must not be null");

    // Method reference
    if (!serializedLambda.getImplMethodName().startsWith("lambda$")) {
      return new InspectionResult(List.of(handleMethodReference(serializedLambda)));
    }

    // Inline lambda
    return handleInlineLambda(serializedLambda);
  }

  private static void binaryOpInt(Deque<Value> operandStack, IntBinaryOperator operation) {
    Object rightOperand =
        resolveValue(popOperand(operandStack, "reading integer operand"), new Object[0]);
    Object leftOperand =
        resolveValue(popOperand(operandStack, "reading integer operand"), new Object[0]);
    if (leftOperand instanceof Integer leftInt && rightOperand instanceof Integer rightInt) {
      operandStack.push(new ConstantValue(operation.applyAsInt(leftInt, rightInt)));
    } else {
      operandStack.push(UnknownValue.INSTANCE);
    }
  }

  private static MethodNode findMethodNode(
      ClassNode classNode, String methodName, String methodDesc) {
    for (MethodNode methodNode : classNode.methods) {
      if (methodNode.name.equals(methodName) && methodNode.desc.equals(methodDesc)) {
        return methodNode;
      }
    }
    return null;
  }

  private static void handleGenericInvoke(
      Deque<Value> operandStack,
      List<InvocationStep> invocationList,
      MethodInsnNode methodNode,
      int opcodeValue,
      Object[] capturedValues) {
    Type[] argumentTypes = Type.getArgumentTypes(methodNode.desc);
    Object[] resolvedArguments = new Object[argumentTypes.length];

    for (int i = argumentTypes.length - 1; i >= 0; i--) {
      resolvedArguments[i] =
          resolveValue(popOperand(operandStack, "reading invocation argument"), capturedValues);
    }

    Object resolvedReceiver = null;
    if (opcodeValue != Opcodes.INVOKESTATIC) {
      resolvedReceiver =
          resolveValue(popOperand(operandStack, "reading invocation receiver"), capturedValues);
    }

    invocationList.add(
        new InvocationStep(
            methodNode.owner,
            methodNode.name,
            methodNode.desc,
            opcodeValue == Opcodes.INVOKESTATIC,
            immutableListAllowingNulls(resolvedArguments),
            resolvedReceiver));

    if (Type.getReturnType(methodNode.desc) != Type.VOID_TYPE) {
      operandStack.push(UnknownValue.INSTANCE);
    }
  }

  @SuppressWarnings({"java:S3776", "java:S6541"})
  private static InspectionResult handleInlineLambda(SerializedLambda serializedLambda) {
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

        case Opcodes.ALOAD, Opcodes.ILOAD, Opcodes.LLOAD, Opcodes.FLOAD, Opcodes.DLOAD -> {
          int varIndex = ((VarInsnNode) node).var;
          if (varIndex < capturedValues.length) {
            operandStack.push(new CapturedValue(varIndex));
          } else {
            operandStack.push(UnknownValue.INSTANCE);
          }
        }

        case Opcodes.IADD -> binaryOpInt(operandStack, Integer::sum);
        case Opcodes.ISUB -> binaryOpInt(operandStack, (a, b) -> a - b);
        case Opcodes.IMUL -> binaryOpInt(operandStack, (a, b) -> a * b);
        case Opcodes.IDIV -> binaryOpInt(operandStack, (a, b) -> a / b);
        case Opcodes.IREM -> binaryOpInt(operandStack, (a, b) -> a % b);

        case Opcodes.POP -> popOperand(operandStack, "executing POP");
        case Opcodes.DUP -> operandStack.push(peekOperand(operandStack, "executing DUP"));

        // Field access: we can't resolve the field value at analysis time, but we can maintain
        // correct stack discipline so the enclosing INVOKE* still pops its arguments and receiver
        // from the right slots. Typical case: a lambda captures `this` and reads an instance
        // field to obtain the invocation receiver (e.g. `this.service.doWork(captured)`).
        case Opcodes.GETFIELD -> {
          popOperand(operandStack, "reading field receiver");
          operandStack.push(UnknownValue.INSTANCE);
        }
        case Opcodes.GETSTATIC -> operandStack.push(UnknownValue.INSTANCE);

        case Opcodes.NEW -> {
          TypeInsnNode typeNode = (TypeInsnNode) node;
          operandStack.push(new NewInstanceMarker(typeNode.desc));
        }
        case Opcodes.INVOKESPECIAL -> {
          MethodInsnNode methodInsn = (MethodInsnNode) node;
          if ("<init>".equals(methodInsn.name)) {
            popInvocationOperands(operandStack, methodInsn);
          } else {
            handleGenericInvoke(
                operandStack, invocationList, methodInsn, opcodeValue, capturedValues);
          }
        }

        case Opcodes.INVOKESTATIC, Opcodes.INVOKEVIRTUAL, Opcodes.INVOKEINTERFACE -> {
          MethodInsnNode methodInsn = (MethodInsnNode) node;
          handleGenericInvoke(
              operandStack, invocationList, methodInsn, opcodeValue, capturedValues);
        }

        // Method return opcodes: RETURN (void) pops nothing; *RETURN pops one value from the
        // stack but the method terminates on the next iteration regardless, so the stack state
        // is irrelevant from here on. Treat them as end-of-analysis no-ops.
        case Opcodes.RETURN -> {
          /* no stack effect visible to subsequent instructions */
        }
        case Opcodes.IRETURN,
            Opcodes.LRETURN,
            Opcodes.FRETURN,
            Opcodes.DRETURN,
            Opcodes.ARETURN -> {
          if (!operandStack.isEmpty()) {
            popOperand(operandStack, "returning value");
          }
        }

        default ->
            // Pushing UnknownValue without popping the operands this instruction consumes would
            // misalign the stack for every subsequent instruction and produce silent wrong
            // answers. Fail fast instead so the caller can fall back to treating the lambda as
            // opaque (serializing it with JDK serialization) rather than trusting a corrupted
            // analysis.
            throw new UnsupportedLambdaBytecodeException(
                "AsmLambdaAnalyzer does not support opcode "
                    + opcodeValue
                    + " (0x"
                    + Integer.toHexString(opcodeValue)
                    + "). Lambda body uses a Java feature the analyzer has not been taught about;"
                    + " either simplify the lambda to a method reference, or extend the analyzer's"
                    + " opcode switch.");
      }
    }

    if (invocationList.isEmpty()) {
      throw new IllegalStateException(
          "Lambda body contained no method calls that could be inspected");
    }

    return new InspectionResult(List.copyOf(invocationList));
  }

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
        List.copyOf(invocationArguments),
        receiver);
  }

  private static List<Object> immutableListAllowingNulls(Object[] values) {
    List<Object> copy = new ArrayList<>(values.length);
    Collections.addAll(copy, values);
    return Collections.unmodifiableList(copy);
  }

  private static void popInvocationOperands(Deque<Value> operandStack, MethodInsnNode methodInsn) {
    Type[] argumentTypes = Type.getArgumentTypes(methodInsn.desc);
    for (int i = argumentTypes.length - 1; i >= 0; i--) {
      popOperand(operandStack, "reading constructor argument");
    }
    popOperand(operandStack, "reading constructor receiver");
  }

  private static Value popOperand(Deque<Value> operandStack, String operation) {
    Value value = operandStack.poll();
    if (value == null) {
      throw stackUnderflow(operation);
    }
    return value;
  }

  private static Value peekOperand(Deque<Value> operandStack, String operation) {
    Value value = operandStack.peek();
    if (value == null) {
      throw stackUnderflow(operation);
    }
    return value;
  }

  private static UnsupportedLambdaBytecodeException stackUnderflow(String operation) {
    return new UnsupportedLambdaBytecodeException(
        "Malformed lambda bytecode: operand stack underflow while " + operation);
  }

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
      throw new IllegalStateException("Cannot read class " + classInternalName, ioException);
    }
  }

  private static Object resolveValue(Value abstractValue, Object[] capturedValues) {
    if (abstractValue instanceof ConstantValue cv) {
      return cv.value();
    } else if (abstractValue instanceof CapturedValue cap) {
      return capturedValues[cap.index()];
    }
    return null;
  }

  private static SerializedLambda toSerializedLambda(Serializable lambda) {
    return LambdaSerialization.toSerializedLambda(lambda, "Lambda is not Serializable");
  }

  @Override
  public LambdaDescriptor analyze(Serializable lambda) {
    Objects.requireNonNull(lambda, "Lambda must not be null");
    SerializedLambda sl = toSerializedLambda(lambda);
    InspectionResult inspection = inspect(sl);
    InvocationStep step = inspection.last();
    return new LambdaDescriptor(
        step.ownerInternalName().replace('/', '.'),
        step.methodName(),
        step.methodDescriptor(),
        step.isStatic(),
        step.arguments().toArray());
  }

  private enum UnknownValue implements Value {
    INSTANCE
  }

  private sealed interface Value
      permits ConstantValue, CapturedValue, NewInstanceMarker, UnknownValue {}

  record InvocationStep(
      String ownerInternalName,
      String methodName,
      String methodDescriptor,
      boolean isStatic,
      List<Object> arguments,
      Object receiver) {}

  record InspectionResult(List<InvocationStep> steps) {

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

  /**
   * Thrown when the analyzer encounters a JVM opcode it does not model. Callers that need to accept
   * arbitrary lambda bodies should catch this and fall back to opaque serialization rather than
   * trusting a partial analysis.
   */
  public static final class UnsupportedLambdaBytecodeException extends IllegalStateException {
    @Serial private static final long serialVersionUID = 1L;

    public UnsupportedLambdaBytecodeException(String message) {
      super(message);
    }
  }
}
