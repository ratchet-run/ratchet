package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.io.Serializable;

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
   * @throws IllegalArgumentException if the lambda is null or cannot be analyzed
   */
  LambdaDescriptor analyze(Serializable lambda);
}
