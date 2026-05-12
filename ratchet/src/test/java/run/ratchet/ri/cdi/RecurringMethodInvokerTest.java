package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecurringMethodInvokerTest {

  @Mock private Instance<Object> allBeans;

  private RecurringMethodInvoker invoker;

  @BeforeEach
  void setUp() {
    invoker = new RecurringMethodInvoker(allBeans, className -> true);
  }

  @Test
  void invoke_runtimeFailure_rethrowsTargetCause() {
    FailingBean bean = new FailingBean();
    selectBean(FailingBean.class, bean);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> invoker.invoke(FailingBean.class.getName(), "failRuntime", false));

    assertSame(bean.runtimeFailure, thrown);
  }

  @Test
  void invoke_checkedFailure_rethrowsTargetCause() {
    FailingBean bean = new FailingBean();
    selectBean(FailingBean.class, bean);

    CheckedFailure thrown =
        assertThrows(
            CheckedFailure.class,
            () -> invoker.invoke(FailingBean.class.getName(), "failChecked", false));

    assertSame(bean.checkedFailure, thrown);
  }

  @Test
  void invoke_dependentScopedBean_destroysHandleAfterInvocation() throws Exception {
    FailingBean bean = new FailingBean();
    Instance.Handle<?> handle = selectBean(FailingBean.class, bean, Dependent.class);

    invoker.invoke(FailingBean.class.getName(), "noop", false);

    verify(handle).destroy();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Instance.Handle<?> selectBean(Class<?> beanClass, Object bean) {
    return selectBean(beanClass, bean, jakarta.enterprise.context.ApplicationScoped.class);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Instance.Handle<?> selectBean(
      Class<?> beanClass, Object bean, Class<? extends java.lang.annotation.Annotation> scope) {
    Instance selected = mock(Instance.class);
    Instance.Handle handle = mock(Instance.Handle.class);
    Bean cdiBean = mock(Bean.class);
    when(allBeans.select((Class) beanClass)).thenReturn(selected);
    when(selected.isUnsatisfied()).thenReturn(false);
    when(selected.getHandle()).thenReturn(handle);
    when(handle.get()).thenReturn(bean);
    when(handle.getBean()).thenReturn(cdiBean);
    doReturn(scope).when(cdiBean).getScope();
    return handle;
  }

  public static final class FailingBean {
    private final RuntimeException runtimeFailure = new RuntimeException("boom");
    private final CheckedFailure checkedFailure = new CheckedFailure("checked");

    public void failRuntime() {
      throw runtimeFailure;
    }

    public void failChecked() throws CheckedFailure {
      throw checkedFailure;
    }

    public void noop() {}
  }

  private static final class CheckedFailure extends Exception {
    private CheckedFailure(String message) {
      super(message);
    }
  }
}
