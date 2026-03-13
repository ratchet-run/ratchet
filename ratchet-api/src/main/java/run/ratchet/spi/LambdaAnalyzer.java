package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.io.Serializable;

/**
 * Analyzes serializable lambda expressions to extract metadata about their target methods and
 * underlying structure.
 *
 * <p>This interface provides a contract for inspecting lambda expressions at runtime.
 * Implementations of this interface are responsible for analyzing the serialized representation of
 * a lambda and extracting details such as the target class, method name, and descriptor. The
 * analysis can handle various types of lambda expressions, including method references and inline
 * lambda bodies.
 *
 * <p>Note: This API is marked as {@link Incubating}, which indicates that it is subject to change
 * and may evolve in incompatible ways in the future without deprecation warnings.
 *
 * @see LambdaDescriptor Describes the extracted metadata from the lambda analysis process.
 */
@Incubating
public interface LambdaAnalyzer {

  /**
   * Analyzes a serializable lambda expression to extract metadata describing its target method and
   * associated characteristics, such as the target class, method name, and captured arguments.
   *
   * @param lambda a serializable lambda expression to be analyzed; must not be null.
   * @return a {@code LambdaDescriptor} containing the extracted metadata from the given lambda
   *     expression. This includes information such as the target class, method name, method
   *     descriptor, whether the method is static, and any captured arguments.
   * @throws IllegalArgumentException if the provided {@code lambda} is null or cannot be analyzed.
   */
  LambdaDescriptor analyze(Serializable lambda);
}
