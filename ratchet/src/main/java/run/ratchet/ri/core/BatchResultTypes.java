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
package run.ratchet.ri.core;

import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.store.entity.JobPayload;

/** Strict, policy-checked type names for persisted batch reducers. */
public final class BatchResultTypes {
  private static final Set<String> VALUES =
      Set.of(
          "java.lang.Long",
          "java.lang.Integer",
          "java.lang.Double",
          "java.lang.Boolean",
          "java.lang.String",
          "java.math.BigDecimal",
          "java.math.BigInteger");
  private static final Set<String> CONTAINERS =
      Set.of("java.util.Map", "java.util.List", "java.util.Set");
  private static final Map<Class<?>, Class<?>> BOXES =
      Map.of(
          long.class,
          Long.class,
          int.class,
          Integer.class,
          double.class,
          Double.class,
          boolean.class,
          Boolean.class,
          byte.class,
          Byte.class,
          short.class,
          Short.class,
          float.class,
          Float.class,
          char.class,
          Character.class,
          void.class,
          Void.class);

  private BatchResultTypes() {}

  public static Type boxed(Type type) {
    return type instanceof Class<?> c ? BOXES.getOrDefault(c, c) : type;
  }

  public static Class<?> raw(Type type) {
    if (type instanceof Class<?> c && !c.isArray()) return c;
    if (type instanceof ParameterizedType p && p.getRawType() instanceof Class<?> c) return c;
    throw new IllegalArgumentException(
        "Result type must be concrete; pass a TypeRef for a generic combiner");
  }

  public static void validate(Type type, ClassPolicy policy) {
    if (type instanceof Class<?> c && !c.isArray() && !c.isPrimitive()) {
      checkName(c.getName(), policy);
    } else if (type instanceof ParameterizedType p) {
      validate(p.getRawType(), policy);
      for (Type argument : p.getActualTypeArguments()) validate(argument, policy);
    } else {
      throw new IllegalArgumentException(
          "Result type contains arrays, wildcards or type variables; pass a concrete TypeRef");
    }
  }

  private static void checkName(String name, ClassPolicy policy) {
    if (!VALUES.contains(name)
        && !CONTAINERS.contains(name)
        && (policy == null || !policy.isAllowed(name) || !policy.isAllowedForResultType(name))) {
      throw new IllegalArgumentException("Batch result class denied by ClassPolicy: " + name);
    }
  }

  public static boolean jdkCombiner(JobPayload payload) {
    return payload.isStatic()
        && switch (payload.target()) {
          case "java.lang.Long", "java.lang.Integer", "java.lang.Double" ->
              "sum".equals(payload.method());
          case "java.lang.Math" -> "max".equals(payload.method()) || "min".equals(payload.method());
          default -> false;
        };
  }

  public static Method method(JobPayload payload, ClassPolicy policy) {
    if (!jdkCombiner(payload) && (policy == null || !policy.isAllowed(payload.target()))) {
      throw new IllegalArgumentException(
          "Batch reducer target denied by ClassPolicy: " + payload.target());
    }
    try {
      Class<?> target =
          Class.forName(payload.target(), false, Thread.currentThread().getContextClassLoader());
      for (Method method : target.getMethods()) {
        if (method.getName().equals(payload.method())
            && MethodType.methodType(method.getReturnType(), method.getParameterTypes())
                .toMethodDescriptorString()
                .equals(payload.methodDescriptor())) return method;
      }
      throw new IllegalArgumentException("Batch target method not found: " + payload.method());
    } catch (ClassNotFoundException ex) {
      throw new IllegalArgumentException("Batch target class not found", ex);
    }
  }

  public static Type parse(String text, ClassPolicy policy) {
    Parser parser = new Parser(text, policy);
    Type type = parser.type();
    parser.space();
    if (parser.position != text.length())
      throw new IllegalArgumentException("Invalid batch result type: " + text);
    return type;
  }

  private static final class Parser {
    private final String text;
    private final ClassPolicy policy;
    private int position;
    private int depth;

    Parser(String text, ClassPolicy policy) {
      this.text = text;
      this.policy = policy;
    }

    void space() {
      while (position < text.length() && text.charAt(position) == ' ') position++;
    }

    Type type() {
      if (++depth > 32)
        throw new IllegalArgumentException("Batch result type is too deeply nested");
      space();
      int start = position;
      while (position < text.length()) {
        char ch = text.charAt(position);
        if (!Character.isJavaIdentifierPart(ch) && ch != '.') break;
        position++;
      }
      if (start == position)
        throw new IllegalArgumentException("Invalid batch result type: " + text);
      String name = text.substring(start, position);
      checkName(name, policy); // Must precede class loading, including nested type arguments.
      Class<?> raw;
      try {
        raw = Class.forName(name, false, Thread.currentThread().getContextClassLoader());
      } catch (ClassNotFoundException ex) {
        throw new IllegalArgumentException("Unknown batch result class: " + name, ex);
      }
      space();
      Type result = raw;
      if (position < text.length() && text.charAt(position) == '<') {
        position++;
        var arguments = new ArrayList<Type>();
        arguments.add(type());
        space();
        while (position < text.length() && text.charAt(position) == ',') {
          position++;
          arguments.add(type());
          space();
        }
        if (position == text.length()
            || text.charAt(position++) != '>'
            || raw.getTypeParameters().length != arguments.size())
          throw new IllegalArgumentException("Invalid type arguments: " + text);
        result = new ConcreteType(raw, arguments.toArray(Type[]::new));
      }
      depth--;
      return result;
    }
  }

  private record ConcreteType(Class<?> raw, Type[] arguments) implements ParameterizedType {
    @Override
    public Type[] getActualTypeArguments() {
      return arguments.clone();
    }

    @Override
    public Type getRawType() {
      return raw;
    }

    @Override
    public Type getOwnerType() {
      return raw.getDeclaringClass();
    }

    @Override
    public String getTypeName() {
      StringJoiner joiner = new StringJoiner(", ", raw.getName() + "<", ">");
      for (Type argument : arguments) joiner.add(argument.getTypeName());
      return joiner.toString();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof ParameterizedType p
          && raw.equals(p.getRawType())
          && Objects.equals(getOwnerType(), p.getOwnerType())
          && Arrays.equals(arguments, p.getActualTypeArguments());
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(arguments) ^ raw.hashCode() ^ Objects.hashCode(getOwnerType());
    }
  }
}
