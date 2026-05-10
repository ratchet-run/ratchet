package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

class DefaultNodeIdentityProviderVisibilityTest {

  @Test
  void lifecycleFieldsReadByHeartbeatThreads_areVolatile() throws Exception {
    assertTrue(
        Modifier.isVolatile(
            DefaultNodeIdentityProvider.class.getDeclaredField("nodeId").getModifiers()));
    assertTrue(
        Modifier.isVolatile(
            DefaultNodeIdentityProvider.class.getDeclaredField("heartbeatHandle").getModifiers()));
  }
}
