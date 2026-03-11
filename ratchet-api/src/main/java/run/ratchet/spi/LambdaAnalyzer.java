package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.io.Serializable;

/** Analyzes serialized lambda expressions to extract target class and method information. */
@Incubating
public interface LambdaAnalyzer {

  LambdaDescriptor analyze(Serializable lambda);
}
