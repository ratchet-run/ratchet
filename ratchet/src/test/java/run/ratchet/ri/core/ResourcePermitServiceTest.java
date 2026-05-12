package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.store.spi.ResourcePermitStore;

@ExtendWith(MockitoExtension.class)
class ResourcePermitServiceTest {

  @Mock private ResourcePermitStore resourcePermitStore;
  @Mock private PollerScheduler pollerScheduler;

  @Test
  void release_wakesPollerWhenStoreReleaseFails() {
    ResourcePermitService service = new ResourcePermitService(resourcePermitStore, pollerScheduler);
    UUID jobId = new UUID(0L, 1L);
    doThrow(new RuntimeException("store unavailable"))
        .when(resourcePermitStore)
        .releasePermit("api", jobId);

    assertThrows(RuntimeException.class, () -> service.release("api", jobId));

    verify(pollerScheduler).wakeup();
  }

  @Test
  void releaseAll_wakesPollerWhenStoreReleaseFails() {
    ResourcePermitService service = new ResourcePermitService(resourcePermitStore, pollerScheduler);
    UUID jobId = new UUID(0L, 2L);
    doThrow(new RuntimeException("store unavailable"))
        .when(resourcePermitStore)
        .releaseAllPermits(jobId);

    assertThrows(RuntimeException.class, () -> service.releaseAll(jobId));

    verify(pollerScheduler).wakeup();
  }
}
