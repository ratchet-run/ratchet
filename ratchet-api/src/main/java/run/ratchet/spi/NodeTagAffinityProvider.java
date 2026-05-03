package run.ratchet.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.NodeTagFilter;

/**
 * SPI for per-node tag affinity. Called once per poll cycle so the filter can change at runtime
 * (e.g., based on hardware availability or load).
 *
 * <p>Return {@link NodeTagFilter#NONE} to disable filtering entirely. The default CDI bean ({@code
 * DefaultNodeTagAffinityProvider}) reads {@code requireTags} / {@code excludeTags} from {@link
 * run.ratchet.api.RatchetOptions.NodeOptions} at construction time.
 */
@Incubating
public interface NodeTagAffinityProvider {

  NodeTagFilter getTagFilter();
}
