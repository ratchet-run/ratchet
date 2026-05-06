package run.ratchet.ri.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.acme.jobs.AcmePredicates;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobResult;
import run.ratchet.api.SerializablePredicate;

class DefaultLambdaSerializerTest {

  @Test
  void applicationPredicateAllowedByClassPolicyRoundTrips() {
    DefaultLambdaSerializer serializer =
        new DefaultLambdaSerializer(className -> className.startsWith("com.acme.jobs."));

    String serialized = serializer.serialize(AcmePredicates.successPredicate());

    SerializablePredicate<JobResult<?>> predicate =
        serializer.deserializeJobResultPredicate(serialized);

    assertNotNull(predicate);
    assertTrue(predicate.test(JobResult.success("ok")));
  }

  @Test
  void applicationPredicateDeniedByClassPolicyIsRejected() {
    DefaultLambdaSerializer serializer = new DefaultLambdaSerializer(className -> false);

    String serialized = serializer.serialize(AcmePredicates.successPredicate());

    assertNull(serializer.deserializeJobResultPredicate(serialized));
  }
}
