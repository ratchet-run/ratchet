package run.ratchet.spi;

import java.io.Serializable;
import run.ratchet.api.Incubating;

/**
 * Extracts target-method metadata from serializable lambda expressions.
 *
 * @see LambdaDescriptor
 */
@Incubating
public interface LambdaAnalyzer {

  /**
   * Analyzes a serializable lambda and returns its target-method metadata.
   *
   * @throws NullPointerException if {@code lambda} is {@code null}
   * @throws IllegalStateException if the lambda cannot be serialized or its bytecode cannot be
   *     analyzed
   */
  LambdaDescriptor analyze(Serializable lambda);
}
