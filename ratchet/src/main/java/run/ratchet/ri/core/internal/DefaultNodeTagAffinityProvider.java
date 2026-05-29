package run.ratchet.ri.core.internal;

import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.NodeTagAffinityProvider;

/**
 * Default {@link NodeTagAffinityProvider} that reads {@code requireTags} / {@code excludeTags} from
 * {@link RatchetOptions.NodeOptions} at construction time and returns the same filter on every
 * call. Produces {@link NodeTagFilter#NONE} when no tags are configured.
 */
public record DefaultNodeTagAffinityProvider(NodeTagFilter tagFilter)
    implements NodeTagAffinityProvider {

  public DefaultNodeTagAffinityProvider(RatchetOptions tagFilter) {
    this(tagFilter.node().tagFilter());
  }
}
