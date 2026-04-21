package run.ratchet.store.util;

import run.ratchet.api.RatchetOptions;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import java.util.List;
import java.util.Locale;
import org.jboss.logging.Logger;

/**
 * Verifies that the database session uses the {@code READ COMMITTED} isolation level required by
 * Ratchet's poll/claim path.
 *
 * <p>Both MySQL ({@code REPEATABLE READ} default) and PostgreSQL can be configured with stricter
 * isolation that breaks claim semantics. Operators don't realize until they hit deadlocks or stale
 * reads in production. This helper runs at {@code @PostConstruct} time in each store implementation
 * and either logs a warning, throws an exception, or skips the check based on {@link
 * RatchetOptions.StoreOptions#isolationCheckMode()}.
 *
 * <h2>Why per-store callers</h2>
 *
 * <p>This helper accepts the detection query and expected value as parameters because each database
 * vendor has a different syntax (and MySQL 5.7 vs 8.0 use different system variable names). Callers
 * pass a list of queries to try in order; the first non-throwing result wins. This allows the MySQL
 * caller to gracefully fall back from {@code @@SESSION.transaction_isolation} (8.0+) to
 * {@code @@SESSION.tx_isolation} (5.7) without any if/else branching here.
 *
 * <p>Critical correctness gotcha: the original {@code @@tx_isolation} system variable was
 * deprecated in MySQL 5.7.20 and removed in MySQL 8.0. Querying it on MySQL 8+ throws "Unknown
 * system variable 'tx_isolation'". An exception-swallowing isolation check would silently treat
 * that as "OK" — the exact opposite of what's intended. The fall-through query strategy here
 * preserves the spirit of "skip on detection failure" only if BOTH queries fail.
 */
public final class IsolationCheck {

  private static final Logger log = Logger.getLogger(IsolationCheck.class);

  private IsolationCheck() {}

  /**
   * Verifies the database session isolation matches the expected value. Tries each query in order;
   * the first one that returns a non-null result is used. If all queries throw, the check is
   * skipped (treated as a detection failure, NOT a misconfiguration).
   *
   * @param em the JPA {@link EntityManager} used to issue native queries
   * @param dbName human-readable database name (e.g. "MySQL", "PostgreSQL") for log messages
   * @param queries one or more native SQL queries to try in order
   * @param expectedValueIgnoreCase the expected isolation value (case-insensitive match, e.g.
   *     "READ-COMMITTED" or "read committed")
   * @param remediation operator-facing fix instructions appended to the warning message
   * @throws IsolationCheckFailedException when fail mode is active and the actual value does not
   *     match
   */
  public static void verifyReadCommitted(
      EntityManager em,
      String dbName,
      List<String> queries,
      String expectedValueIgnoreCase,
      String remediation) {
    verifyReadCommitted(
        em,
        dbName,
        queries,
        expectedValueIgnoreCase,
        remediation,
        RatchetOptions.IsolationCheckMode.FAIL);
  }

  public static void verifyReadCommitted(
      EntityManager em,
      String dbName,
      List<String> queries,
      String expectedValueIgnoreCase,
      String remediation,
      RatchetOptions.IsolationCheckMode mode) {
    if (mode == RatchetOptions.IsolationCheckMode.DISABLE) {
      log.debugf("%s isolation check disabled by RatchetOptions", dbName);
      return;
    }

    String actual = null;
    Exception lastFailure = null;
    for (String query : queries) {
      try {
        Object result = em.createNativeQuery(query).getSingleResult();
        actual = result != null ? result.toString() : null;
        if (actual != null) {
          break;
        }
      } catch (PersistenceException | IllegalStateException e) {
        lastFailure = e;
      }
    }

    if (actual == null) {
      // Detection failure across every query — be permissive (skip), but log at debug so
      // operators investigating mysterious behavior can find the trail.
      String reason = lastFailure != null ? lastFailure.getMessage() : "no result";
      log.debugf("Could not check %s isolation level: %s", dbName, reason);
      return;
    }

    if (!normalize(expectedValueIgnoreCase).equals(normalize(actual))) {
      String message =
          String.format(
              "%s session isolation is '%s' — Ratchet requires %s. %s",
              dbName, actual, expectedValueIgnoreCase, remediation);
      if (mode == RatchetOptions.IsolationCheckMode.FAIL) {
        throw new IsolationCheckFailedException(message);
      }
      log.warn(message);
    }
  }

  /**
   * Normalizes an isolation value for comparison. Different databases use different separators
   * ({@code READ-COMMITTED} on MySQL, {@code read committed} on PostgreSQL); this collapses both to
   * the same canonical form so a single {@code expectedValue} parameter works.
   */
  private static String normalize(String s) {
    return s.toLowerCase(Locale.ROOT).replace('-', ' ').replaceAll("\\s+", " ").trim();
  }
}
