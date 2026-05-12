package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
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

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void selectBean(Class<?> beanClass, Object bean) {
    Instance selected = mock(Instance.class);
    when(allBeans.select((Class) beanClass)).thenReturn(selected);
    when(selected.isUnsatisfied()).thenReturn(false);
    when(selected.get()).thenReturn(bean);
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
  }

  private static final class CheckedFailure extends Exception {
    private CheckedFailure(String message) {
      super(message);
    }
  }
}
