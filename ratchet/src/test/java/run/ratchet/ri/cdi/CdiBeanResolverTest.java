package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import org.junit.jupiter.api.Test;

class CdiBeanResolverTest {

  @Test
  void resolve_defaultDependentScopeFromCdiBeanMetadata_isRejected() {
    Instance<Object> allBeans = mock(Instance.class);
    Instance<DefaultScopedBean> selected = selected(DefaultScopedBean.class, Dependent.class);
    when(allBeans.select(DefaultScopedBean.class)).thenReturn(selected);

    CdiBeanResolver resolver = new CdiBeanResolver(allBeans);

    assertThrows(IllegalStateException.class, () -> resolver.resolve(DefaultScopedBean.class));
  }

  @Test
  void resolve_applicationScopedBean_returnsHandleValue() {
    Instance<Object> allBeans = mock(Instance.class);
    ApplicationBean bean = new ApplicationBean();
    Instance<ApplicationBean> selected =
        selected(ApplicationBean.class, ApplicationScoped.class, bean);
    when(allBeans.select(ApplicationBean.class)).thenReturn(selected);

    CdiBeanResolver resolver = new CdiBeanResolver(allBeans);

    assertSame(bean, resolver.resolve(ApplicationBean.class));
  }

  private static <T> Instance<T> selected(
      Class<T> type, Class<? extends java.lang.annotation.Annotation> scope) {
    return selected(type, scope, null);
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> selected(
      Class<T> type, Class<? extends java.lang.annotation.Annotation> scope, T beanInstance) {
    Instance<T> instance = mock(Instance.class);
    Instance.Handle<T> handle = mock(Instance.Handle.class);
    Bean<T> bean = mock(Bean.class);
    when(instance.isUnsatisfied()).thenReturn(false);
    when(instance.isAmbiguous()).thenReturn(false);
    when(instance.getHandle()).thenReturn(handle);
    when(handle.getBean()).thenReturn(bean);
    doReturn(scope).when(bean).getScope();
    if (beanInstance != null) {
      when(handle.get()).thenReturn(beanInstance);
    }
    return instance;
  }

  static class DefaultScopedBean {}

  @ApplicationScoped
  static class ApplicationBean {}
}
