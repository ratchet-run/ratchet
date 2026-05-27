package run.ratchet.testsuite.concurrency;

import jakarta.enterprise.concurrent.ManagedExecutorDefinition;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Declares a virtual-thread {@code ManagedExecutorService} as an <b>application</b> component (it
 * lands in {@code WEB-INF/classes}), following the standard Jakarta Concurrency 3.1 pattern:
 * {@code @ManagedExecutorDefinition(virtual = true)} on an {@code @ApplicationScoped} class.
 *
 * <p>Ratchet ships no executor definition of its own (the library stays EE-agnostic), so the
 * application owning the executor is the supported pattern. This is what a real deployment does:
 * declare the executor, then set {@code ratchet.worker.job-executor-jndi} to its name. {@link
 * VirtualThreadOptionsProducer} does exactly that for the test. Declaring it as a WAR class also
 * avoids depending on whether a container scans {@code WEB-INF/lib} for resource definitions
 * (GlassFish does not).
 */
@ManagedExecutorDefinition(name = "java:app/concurrent/RatchetTestVirtualExecutor", virtual = true)
@ApplicationScoped
public class VirtualThreadTestExecutor {}
