package run.ratchet.store.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.objectweb.asm.Type;

/**
 * Represents the payload information for a job, encapsulating all details needed to invoke a method
 * via reflection.
 *
 * <p>This record provides a structured, serializable representation of a method invocation,
 * including the target class, method name, method descriptor, invocation type (static vs instance),
 * and the actual arguments.
 *
 * @param target the fully qualified name of the target class
 * @param method the name of the method to be invoked
 * @param methodDescriptor the JVM method descriptor defining parameter and return types
 * @param isStatic true if the method is static, false for instance methods
 * @param args the list of arguments to pass to the method (must match descriptor)
 */
public record JobPayload(
    String target, String method, String methodDescriptor, boolean isStatic, List<Object> args)
    implements Serializable {

  /**
   * Mapping from JVM primitive type descriptor characters to their corresponding Java Class
   * objects.
   */
  private static final Map<Character, Class<?>> PRIMITIVE_TYPES =
      Map.of(
          'Z', boolean.class,
          'B', byte.class,
          'C', char.class,
          'S', short.class,
          'I', int.class,
          'J', long.class,
          'F', float.class,
          'D', double.class);

  /**
   * Extracts the parameter types from the method descriptor.
   *
   * @return an array of Class objects representing the parameter types
   * @throws IllegalStateException if parameter types cannot be resolved
   */
  public Class<?>[] parameterTypes() {
    Type[] asmTypes = Type.getArgumentTypes(methodDescriptor);
    Class<?>[] clz = new Class<?>[asmTypes.length];
    try {
      for (int i = 0; i < asmTypes.length; i++) {
        clz[i] = resolveType(asmTypes[i]);
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Unable to resolve parameter types for job payload", e);
    }
    return clz;
  }

  /**
   * Resolves an ASM {@link Type} to its corresponding Java {@link Class} object.
   *
   * @param type the ASM Type to resolve
   * @return the corresponding Java Class
   * @throws ClassNotFoundException if the class cannot be found
   */
  private static Class<?> resolveType(Type type) throws ClassNotFoundException {
    return switch (type.getSort()) {
      case Type.VOID -> void.class;
      case Type.BOOLEAN,
          Type.BYTE,
          Type.CHAR,
          Type.SHORT,
          Type.INT,
          Type.LONG,
          Type.FLOAT,
          Type.DOUBLE ->
          PRIMITIVE_TYPES.get(type.getDescriptor().charAt(0));
      case Type.ARRAY -> Class.forName(type.getDescriptor().replace('/', '.'));
      case Type.OBJECT -> Class.forName(type.getClassName());
      default -> throw new IllegalArgumentException("Unsupported ASM type sort: " + type.getSort());
    };
  }
}
