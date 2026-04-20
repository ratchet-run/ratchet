package run.ratchet.ri.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.acme.jobs.AcmePredicates;
import run.ratchet.api.JobResult;
import run.ratchet.api.SerializablePredicate;
import org.junit.jupiter.api.Test;

class LambdaSerializerTest {

  @Test
  void applicationPredicateAllowedByClassPolicyRoundTrips() {
    LambdaSerializer serializer =
        new LambdaSerializer(className -> className.startsWith("com.acme.jobs."));

    String serialized = serializer.serialize(AcmePredicates.successPredicate());

    SerializablePredicate<JobResult<?>> predicate =
        serializer.deserializeJobResultPredicate(serialized);

    assertNotNull(predicate);
    assertTrue(predicate.test(JobResult.success("ok")));
  }

  @Test
  void applicationPredicateDeniedByClassPolicyIsRejected() {
    LambdaSerializer serializer = new LambdaSerializer(className -> false);

    String serialized = serializer.serialize(AcmePredicates.successPredicate());

    assertNull(serializer.deserializeJobResultPredicate(serialized));
  }
}
