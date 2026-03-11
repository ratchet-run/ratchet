package run.ratchet.ri.cdi;

import run.ratchet.spi.BeanResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

/**
 * CDI implementation of {@link BeanResolver} that resolves bean instances via the CDI container.
 *
 * @see BeanResolver
 */
@ApplicationScoped
public class CdiBeanResolver implements BeanResolver {

  @Inject @Any private Instance<Object> allBeans;

  @Override
  public <T> T resolve(Class<T> type) {
    Instance<T> instance = allBeans.select(type);
    if (instance.isUnsatisfied()) {
      throw new IllegalStateException("No CDI bean found for type: " + type.getName());
    }
    return instance.get();
  }
}
