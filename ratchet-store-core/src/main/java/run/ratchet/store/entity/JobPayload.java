package run.ratchet.store.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.objectweb.asm.Type;

/** Serializable payload describing a method invocation for job execution. */
public record JobPayload(
    String target, String method, String methodDescriptor, boolean isStatic, List<Object> args)
    implements Serializable {

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
      case Type.ARRAY ->
          Class.forName(
              type.getDescriptor().replace('/', '.'),
              true,
              Thread.currentThread().getContextClassLoader());
      case Type.OBJECT ->
          Class.forName(type.getClassName(), true, Thread.currentThread().getContextClassLoader());
      default -> throw new IllegalArgumentException("Unsupported ASM type sort: " + type.getSort());
    };
  }
}
