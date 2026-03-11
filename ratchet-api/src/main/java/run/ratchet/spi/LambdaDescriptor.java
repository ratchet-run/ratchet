package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Describes a lambda expression's target method for serialization and execution. */
@Incubating
public record LambdaDescriptor(
    String targetClass,
    String methodName,
    String methodDescriptor,
    boolean isStatic,
    Object[] capturedArgs) {}
