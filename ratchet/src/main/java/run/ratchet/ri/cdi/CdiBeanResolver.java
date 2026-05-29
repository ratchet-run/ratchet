package run.ratchet.ri.cdi;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import run.ratchet.spi.BeanResolver;

/**
 * Resolves CDI beans by type via {@link Instance}. Throws {@link IllegalStateException} if no bean
 * or multiple beans are found, and refuses {@link Dependent}-scoped beans whose lifecycle it cannot
 * manage.
 */
@ApplicationScoped
class CdiBeanResolver implements BeanResolver {

  private final Instance<Object> allBeans;

  protected CdiBeanResolver() {
    this.allBeans = null;
  }

  @Inject
  public CdiBeanResolver(@Any Instance<Object> allBeans) {
    this.allBeans = allBeans;
  }

  @Override
  public <T> T resolve(Class<T> type) {
    Instance<T> instance = allBeans.select(type);
    if (instance.isUnsatisfied()) {
      throw new IllegalStateException("No CDI bean found for type: " + type.getName());
    }
    if (instance.isAmbiguous()) {
      throw new IllegalStateException(
          "Multiple CDI beans found for type: "
              + type.getName()
              + ". Use a qualifier to disambiguate.");
    }
    Instance.Handle<T> handle = instance.getHandle();
    if (handle.getBean().getScope().equals(Dependent.class)) {
      throw new IllegalStateException(
          "Cannot resolve @Dependent-scoped bean for type: "
              + type.getName()
              + ". BeanResolver does not manage the lifecycle of @Dependent beans."
              + " Inject the bean directly or use a wider scope.");
    }
    return handle.get();
  }
}
