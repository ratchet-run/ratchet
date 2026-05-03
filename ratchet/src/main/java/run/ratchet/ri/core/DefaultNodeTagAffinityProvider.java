package run.ratchet.ri.core;

import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.NodeTagAffinityProvider;

/**
 * Default {@link NodeTagAffinityProvider} that reads {@code requireTags} / {@code excludeTags} from
 * {@link RatchetOptions.NodeOptions} at construction time and returns the same filter on every
 * call. Produces {@link NodeTagFilter#NONE} when no tags are configured.
 */
public class DefaultNodeTagAffinityProvider implements NodeTagAffinityProvider {

  private final NodeTagFilter tagFilter;

  public DefaultNodeTagAffinityProvider(RatchetOptions options) {
    this.tagFilter = options.node().tagFilter();
  }

  @Override
  public NodeTagFilter getTagFilter() {
    return tagFilter;
  }
}
