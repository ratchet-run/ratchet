/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.spi.BeanResolver;

@ExtendWith(MockitoExtension.class)
class RecurringMethodInvokerTest {

  @Mock private BeanResolver beanResolver;

  private RecurringMethodInvoker invoker;

  @BeforeEach
  void setUp() {
    invoker = new RecurringMethodInvoker(beanResolver, className -> true);
  }

  @Test
  void invoke_runtimeFailure_rethrowsTargetCause() {
    FailingBean bean = new FailingBean();
    BeanResolver.ManagedBeanHandle<?> handle = selectBean(FailingBean.class, bean);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> invoker.invoke(FailingBean.class.getName(), "failRuntime", false));

    assertSame(bean.runtimeFailure, thrown);
    verify(handle).close();
  }

  @Test
  void invoke_checkedFailure_rethrowsTargetCause() {
    FailingBean bean = new FailingBean();
    BeanResolver.ManagedBeanHandle<?> handle = selectBean(FailingBean.class, bean);

    CheckedFailure thrown =
        assertThrows(
            CheckedFailure.class,
            () -> invoker.invoke(FailingBean.class.getName(), "failChecked", false));

    assertSame(bean.checkedFailure, thrown);
    verify(handle).close();
  }

  @Test
  void invoke_managedBean_closesHandleAfterSuccessfulInvocation() throws Exception {
    FailingBean bean = new FailingBean();
    BeanResolver.ManagedBeanHandle<?> handle = selectBean(FailingBean.class, bean);

    invoker.invoke(FailingBean.class.getName(), "noop", false);

    verify(handle).close();
  }

  @Test
  void invoke_inheritedMethod_walksSuperclassChain() throws Exception {
    InheritedBean bean = new InheritedBean();
    selectBean(InheritedBean.class, bean);

    invoker.invoke(InheritedBean.class.getName(), "inherited", false);

    org.junit.jupiter.api.Assertions.assertEquals(1, bean.invocations.get());
  }

  @Test
  void invoke_covariantOverride_ignoresBridgeMethod() throws Exception {
    CovariantBean bean = new CovariantBean();
    selectBean(CovariantBean.class, bean);

    invoker.invoke(CovariantBean.class.getName(), "value", false);

    org.junit.jupiter.api.Assertions.assertEquals(1, bean.invocations.get());
  }

  @Test
  void invoke_jdkProxy_resolvesMethodAgainstRuntimeProxyClass() throws Exception {
    AtomicInteger invocations = new AtomicInteger();
    RecurringContract proxy =
        (RecurringContract)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {RecurringContract.class},
                (ignored, method, args) -> {
                  if (method.getName().equals("run")) {
                    invocations.incrementAndGet();
                  }
                  return null;
                });
    selectBean(RecurringBean.class, proxy);

    invoker.invoke(RecurringBean.class.getName(), "run", false);

    org.junit.jupiter.api.Assertions.assertEquals(1, invocations.get());
  }

  @Test
  void invoke_handleGetFailure_stillClosesHandle() {
    BeanResolver.ManagedBeanHandle<?> handle = mock(BeanResolver.ManagedBeanHandle.class);
    when(beanResolver.resolveManaged(FailingBean.class)).thenReturn(cast(handle));
    when(handle.get()).thenThrow(new IllegalStateException("creation failed"));

    assertThrows(
        IllegalStateException.class,
        () -> invoker.invoke(FailingBean.class.getName(), "noop", false));

    verify(handle).close();
  }

  @Test
  void invoke_classPolicyDenial_happensBeforeBeanResolution() {
    invoker = new RecurringMethodInvoker(beanResolver, className -> false);

    assertThrows(
        SecurityException.class, () -> invoker.invoke(FailingBean.class.getName(), "noop", false));

    verifyNoInteractions(beanResolver);
  }

  @Test
  void validateBeanResolvable_closesManagedHandle() {
    FailingBean bean = new FailingBean();
    BeanResolver.ManagedBeanHandle<?> handle = selectBean(FailingBean.class, bean);

    invoker.validateBeanResolvable(FailingBean.class);

    verify(handle).close();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private BeanResolver.ManagedBeanHandle<?> selectBean(Class<?> beanClass, Object bean) {
    BeanResolver.ManagedBeanHandle handle = mock(BeanResolver.ManagedBeanHandle.class);
    when(beanResolver.resolveManaged((Class) beanClass)).thenReturn(handle);
    when(handle.get()).thenReturn(bean);
    return handle;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static BeanResolver.ManagedBeanHandle cast(BeanResolver.ManagedBeanHandle<?> handle) {
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

  public static class InheritedBase {
    protected final AtomicInteger invocations = new AtomicInteger();

    public void inherited() {
      invocations.incrementAndGet();
    }
  }

  public static final class InheritedBean extends InheritedBase {}

  public static class CovariantBase {
    public Number value() {
      return 0;
    }
  }

  public static final class CovariantBean extends CovariantBase {
    private final AtomicInteger invocations = new AtomicInteger();

    @Override
    public Integer value() {
      invocations.incrementAndGet();
      return 1;
    }
  }

  public interface RecurringContract {
    void run();
  }

  public static final class RecurringBean implements RecurringContract {
    @Override
    public void run() {}
  }

  private static final class CheckedFailure extends Exception {
    private CheckedFailure(String message) {
      super(message);
    }
  }
}
