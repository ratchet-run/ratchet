package run.ratchet.ri.cdi;

import run.ratchet.spi.BeanResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

/**
 * Resolves CDI beans by their type through the {@link BeanResolver} interface. It abstracts the
 * mechanism for dependency injection via CDI and provides runtime resolution of bean instances.
 * <br>
 * This class uses CDI's {@link Instance} to perform type-safe bean resolution. If no bean or
 * multiple beans of the given type are found, it throws an {@link IllegalStateException} to
 * indicate the issue. <br>
 * It supports both direct and qualified dependency resolution through the standard CDI mechanism.
 * Typically, this class is used to resolve application-scoped CDI beans dynamically at runtime.
 */
@ApplicationScoped
public class CdiBeanResolver implements BeanResolver {

  /**
   * Represents a CDI {@link Instance} that holds all available CDI beans in the application
   * context. Provides a means to dynamically resolve and interact with CDI-managed beans at
   * runtime. The {@link Instance} abstraction ensures type-safe querying and retrieval of beans.
   *
   * <p>This variable is injected with the {@code @Any} annotation, indicating that it reflects the
   * entire set of CDI beans without restriction, enabling runtime selection and resolution
   * capabilities. It serves as a fundamental mechanism for supporting dynamic and contextual
   * dependency injection.
   */
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
    if (allBeans == null) {
      throw new IllegalStateException("CDI bean resolver not initialized");
    }
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
    return instance.get();
  }
}
