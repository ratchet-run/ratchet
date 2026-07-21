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
package run.ratchet.testsuite.app;

import jakarta.enterprise.inject.spi.CDI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;

/** Job bodies and callbacks used by caller-principal inheritance integration tests. */
public class CallerPrincipalInheritanceJob {

  private static final AtomicReference<UUID> CHILD_ID = new AtomicReference<>();
  private static final AtomicReference<String> SUCCESS_CALLBACK_PRINCIPAL = new AtomicReference<>();
  private static final AtomicReference<String> FAILURE_CALLBACK_PRINCIPAL = new AtomicReference<>();

  public static void submitChildOnWorkerThread() {
    JobSchedulerService scheduler = CDI.current().select(JobSchedulerService.class).get();
    JobHandle child = scheduler.enqueue(CallerPrincipalInheritanceJob::child).submit();
    CHILD_ID.set(child.id());
  }

  public static void child() {}

  public static void succeed() {}

  public static void fail() {
    throw new IllegalStateException("intentional failure for callback context test");
  }

  public static void captureSuccessCallbackContext() {
    JobContext context = JobContext.currentOrNull();
    SUCCESS_CALLBACK_PRINCIPAL.set(context != null ? context.callerPrincipal() : null);
  }

  public static void captureFailureCallbackContext() {
    JobContext context = JobContext.currentOrNull();
    FAILURE_CALLBACK_PRINCIPAL.set(context != null ? context.callerPrincipal() : null);
  }

  public static UUID childId() {
    return CHILD_ID.get();
  }

  public static String successCallbackPrincipal() {
    return SUCCESS_CALLBACK_PRINCIPAL.get();
  }

  public static String failureCallbackPrincipal() {
    return FAILURE_CALLBACK_PRINCIPAL.get();
  }

  public static void reset() {
    CHILD_ID.set(null);
    SUCCESS_CALLBACK_PRINCIPAL.set(null);
    FAILURE_CALLBACK_PRINCIPAL.set(null);
  }
}
