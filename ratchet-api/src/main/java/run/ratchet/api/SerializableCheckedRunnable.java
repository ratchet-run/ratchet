package run.ratchet.api;

import java.io.Serializable;

/**
 * Serializable variant of {@link Runnable} that can throw checked exceptions.
 *
 * <p>This is the primary functional interface for defining job tasks in the scheduler. Unlike
 * standard {@link Runnable}, this interface allows the {@code run()} method to throw checked
 * exceptions, which are properly handled by the job execution framework for retry logic, error
 * reporting, and dead letter queue processing.
 *
 * <h2>Key Characteristics:</h2>
 *
 * <ul>
 *   <li>Serializable for persistence in job queues
 *   <li>Supports checked exception propagation
 *   <li>Zero-argument execution model
 *   <li>Foundation for all job types in the scheduler
 * </ul>
 *
 * <h2>IMPORTANT: Method Reference Constraint</h2>
 *
 * <p><strong>Due to serialization constraints, job lambdas must contain exactly one method
 * invocation (single method reference or method call).</strong> Multi-statement lambdas will fail
 * at submission time with an {@code IllegalArgumentException}.
 *
 * <h3>Correct Usage (Method References):</h3>
 *
 * <pre>{@code
 * // Static method reference
 * scheduler.enqueue(MyService::processData).submit();
 *
 * // Instance method reference
 * scheduler.enqueue(myService::sendEmail).submit();
 *
 * // Single method call with parameters
 * scheduler.enqueue(() -> myService.process(userId)).submit();
 *
 * // Single method call with multiple parameters
 * scheduler.enqueue(() -> reportService.generate(userId, reportType, startDate)).submit();
 * }</pre>
 *
 * <h3>Incorrect Usage (Multi-Statement Lambdas):</h3>
 *
 * <pre>{@code
 * // WRONG - Will throw IllegalArgumentException at submission time
 * scheduler.enqueue(() -> {
 *     processData();
 *     updateDatabase();
 * }).submit();
 *
 * // WRONG - Multiple invocations not supported
 * scheduler.enqueue(() -> {
 *     User user = userService.findById(userId);
 *     notificationService.send(user, message);
 * }).submit();
 * }</pre>
 *
 * <h3>Workaround for Complex Logic:</h3>
 *
 * <p>For multi-step operations, create a dedicated method in a CDI-managed bean and reference it:
 *
 * <pre>{@code
 * @ApplicationScoped
 * public class UserJobHandler {
 *     @Inject UserService userService;
 *     @Inject NotificationService notificationService;
 *     @Inject DatabaseService databaseService;
 *
 *     public void processUserData(String userId) {
 *         // Complex multi-step logic here
 *         User user = userService.findById(userId);
 *         if (user != null) {
 *             processData(user);
 *             updateDatabase(user);
 *             notifyUser(user);
 *         }
 *     }
 * }
 *
 * // Usage - single method reference
 * scheduler.enqueue(() -> userJobHandler.processUserData(userId)).submit();
 * }</pre>
 *
 * <h2>Exception Handling:</h2>
 *
 * <p>Exceptions thrown from {@code run()} are caught by the job runner and trigger:
 *
 * <ul>
 *   <li>Retry attempts based on job configuration
 *   <li>Backoff delays between retries
 *   <li>Dead letter queue processing after max retries
 *   <li>Error callbacks if configured
 * </ul>
 *
 * <h2>Serialization Considerations:</h2>
 *
 * <p>When using lambda expressions, ensure all captured variables are serializable. Common pitfalls
 * include capturing non-serializable services or resources.
 *
 * @see JobSchedulerService#enqueue(SerializableCheckedRunnable)
 * @see JobBuilder
 * @see SerializableConsumer
 */
@FunctionalInterface
@SuppressWarnings("java:S112")
// Generic exception intentional for job execution functional interface
public interface SerializableCheckedRunnable extends Serializable {

  /**
   * Executes the job task.
   *
   * <p>This method contains the actual work to be performed by the job. It may throw any exception,
   * which will be handled by the job execution framework according to the configured retry and
   * error handling policies.
   *
   * @throws Exception any exception that occurs during job execution
   */
  void run() throws Exception;
}
