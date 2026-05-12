package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_NODE;

import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.exception.RatchetTransientStoreException;

class MongoExceptionTranslationTest {

  @Test
  void claimNextBatchTranslatesTransientCandidateFailure() {
    MongoSocketException failure = transientFailure();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("aggregate".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoJobClaimOperations claims = new MongoJobClaimOperations(contextWithJobs(jobs));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () -> claims.claimNextBatch(1, "node-1", NodeTagFilter.NONE));

    assertSame(failure, thrown.getCause());
  }

  @Test
  void claimCandidateCursorClosesWhenIterationFails() {
    AtomicBoolean closed = new AtomicBoolean();
    MongoCursor<Document> cursor = cursor(List.of(new Document(ID, "not-a-uuid")), closed);
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("aggregate".equals(method.getName())) {
                return aggregateIterable(cursor);
              }
              return defaultValue(method);
            });
    MongoJobClaimOperations claims = new MongoJobClaimOperations(contextWithJobs(jobs));

    assertThrows(
        RuntimeException.class, () -> claims.claimNextBatch(1, "node-1", NodeTagFilter.NONE));

    assertTrue(closed.get());
  }

  @Test
  void claimReadBackCursorClosesWhenMapperFails() {
    UUID id = UUID.randomUUID();
    AtomicBoolean readBackClosed = new AtomicBoolean();
    MongoCursor<Document> candidateCursor =
        cursor(List.of(new Document(ID, id)), new AtomicBoolean());
    MongoCursor<Document> readBackCursor = cursor(List.of(new Document(ID, id)), readBackClosed);
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "aggregate" -> aggregateIterable(candidateCursor);
                case "bulkWrite" -> BulkWriteResult.acknowledged(0, 1, 0, 1, List.of(), List.of());
                case "find" -> findIterable(readBackCursor);
                default -> defaultValue(method);
              };
            });
    MongoJobClaimOperations claims = new MongoJobClaimOperations(contextWithJobs(jobs));

    assertThrows(
        RuntimeException.class, () -> claims.claimNextBatch(1, "node-1", NodeTagFilter.NONE));

    assertTrue(readBackClosed.get());
  }

  @Test
  void lifecycleMutationTranslatesTransientMongoFailure() {
    MongoSocketException failure = transientFailure();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("updateOne".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoStoreContext ctx = contextWithJobs(jobs);
    MongoJobLifecycleOperations lifecycle =
        new MongoJobLifecycleOperations(ctx, new MongoBatchOperations(ctx));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () -> lifecycle.updateJobStatus(UUID.randomUUID(), JobStatus.FAILED, "failed"));

    assertSame(failure, thrown.getCause());
  }

  @Test
  void transactionalLifecycleBodyKeepsMongoExceptionVisibleToSession() {
    MongoSocketException failure = transientFailure();
    AtomicReference<RuntimeException> seenBySession = new AtomicReference<>();
    MongoCollection<Document> jobs =
        mongoCollection(
            (proxy, method, args) -> {
              if ("updateOne".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                try {
                  return ((TransactionBody<?>) args[0]).execute();
                } catch (RuntimeException e) {
                  seenBySession.set(e);
                  throw e;
                }
              }
              return defaultValue(method);
            });
    MongoStoreContext ctx = contextWithJobsAndSession(jobs, session);
    MongoJobLifecycleOperations lifecycle =
        new MongoJobLifecycleOperations(ctx, new MongoBatchOperations(ctx));

    assertThrows(
        RatchetTransientStoreException.class,
        () ->
            lifecycle.markJobSucceededAndUpdateBatch(
                UUID.randomUUID(),
                "{}",
                "json",
                Instant.now(),
                Instant.now(),
                1L,
                1L,
                UUID.randomUUID()));

    assertSame(failure, seenBySession.get());
  }

  @Test
  void deliverSignalByKeyTranslatesTransientTransactionFailure() {
    MongoSocketException failure = transientFailure();
    ClientSession session =
        clientSession(
            (proxy, method, args) -> {
              if ("withTransaction".equals(method.getName())) {
                throw failure;
              }
              return defaultValue(method);
            });
    MongoSignalOperations signals =
        new MongoSignalOperations(contextWithJobsAndSession(null, session));

    RatchetTransientStoreException thrown =
        assertThrows(
            RatchetTransientStoreException.class,
            () ->
                signals.deliverSignalByKey(
                    "approval",
                    "{}",
                    "json",
                    "APPROVED",
                    null,
                    "tester",
                    Instant.now(),
                    "delivery-1"));

    assertSame(failure, thrown.getCause());
  }

  @Test
  void tryLockReadsDatabaseTimeBeforeWritingLease() {
    AtomicBoolean readDatabaseTime = new AtomicBoolean();
    MongoCollection<Document> locks =
        mongoCollection(
            (proxy, method, args) -> {
              if ("findOneAndUpdate".equals(method.getName())) {
                return new Document(OWNER_NODE, "node-1");
              }
              return defaultValue(method);
            });
    MongoDatabase database =
        mongoDatabase(
            (proxy, method, args) -> {
              if ("runCommand".equals(method.getName())) {
                readDatabaseTime.set(true);
                return new Document(
                    "localTime", DocumentMapper.toDate(Instant.parse("2040-01-01T00:00:00Z")));
              }
              if ("getCollection".equals(method.getName())) {
                return locks;
              }
              return defaultValue(method);
            });
    MongoNodeLockOperations locksOps =
        new MongoNodeLockOperations(new MongoStoreContext(mongoClient(null), database));

    assertTrue(locksOps.tryLock("jobArchiver", java.time.Duration.ofMinutes(5), "node-1"));

    assertTrue(readDatabaseTime.get());
  }

  private static MongoStoreContext contextWithJobs(MongoCollection<Document> jobs) {
    return contextWithJobsAndSession(jobs, null);
  }

  private static MongoStoreContext contextWithJobsAndSession(
      MongoCollection<Document> jobs, ClientSession session) {
    MongoDatabase database =
        mongoDatabase(
            (proxy, method, args) -> {
              if ("getCollection".equals(method.getName())) {
                return jobs;
              }
              return defaultValue(method);
            });
    return new MongoStoreContext(mongoClient(session), database);
  }

  private static MongoSocketException transientFailure() {
    return new MongoSocketException("network failure", new ServerAddress());
  }

  private static AggregateIterable<Document> aggregateIterable(MongoCursor<Document> cursor) {
    return proxy(
        AggregateIterable.class,
        (proxy, method, args) -> {
          return switch (method.getName()) {
            case "allowDiskUse", "hintString" -> proxy;
            case "cursor", "iterator" -> cursor;
            default -> defaultValue(method);
          };
        });
  }

  private static FindIterable<Document> findIterable(MongoCursor<Document> cursor) {
    return proxy(
        FindIterable.class,
        (proxy, method, args) -> {
          return switch (method.getName()) {
            case "sort", "limit", "projection" -> proxy;
            case "cursor", "iterator" -> cursor;
            default -> defaultValue(method);
          };
        });
  }

  private static MongoCursor<Document> cursor(List<Document> documents, AtomicBoolean closed) {
    AtomicInteger index = new AtomicInteger();
    return proxy(
        MongoCursor.class,
        (proxy, method, args) -> {
          return switch (method.getName()) {
            case "hasNext" -> index.get() < documents.size();
            case "next" -> documents.get(index.getAndIncrement());
            case "close" -> {
              closed.set(true);
              yield null;
            }
            default -> defaultValue(method);
          };
        });
  }

  private static MongoClient mongoClient(ClientSession session) {
    return proxy(
        MongoClient.class,
        (proxy, method, args) -> {
          if ("startSession".equals(method.getName())) {
            return session;
          }
          return defaultValue(method);
        });
  }

  private static ClientSession clientSession(InvocationHandler handler) {
    return proxy(ClientSession.class, handler);
  }

  private static MongoCollection<Document> mongoCollection(InvocationHandler handler) {
    return proxy(MongoCollection.class, handler);
  }

  private static MongoDatabase mongoDatabase(InvocationHandler handler) {
    return proxy(MongoDatabase.class, handler);
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(Class<?> type, InvocationHandler handler) {
    return (T)
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              Object objectResult = objectMethodResult(proxy, method, args);
              if (objectResult != NO_OBJECT_METHOD) {
                return objectResult;
              }
              return handler.invoke(proxy, method, args);
            });
  }

  private static final Object NO_OBJECT_METHOD = new Object();

  private static Object objectMethodResult(Object proxy, Method method, Object[] args) {
    if (method.getDeclaringClass() != Object.class) {
      return NO_OBJECT_METHOD;
    }
    return switch (method.getName()) {
      case "toString" -> proxy.getClass().getInterfaces()[0].getSimpleName() + " proxy";
      case "hashCode" -> System.identityHashCode(proxy);
      case "equals" -> proxy == args[0];
      default -> NO_OBJECT_METHOD;
    };
  }

  private static Object defaultValue(Method method) {
    Class<?> returnType = method.getReturnType();
    if (!returnType.isPrimitive()) {
      return null;
    }
    if (returnType == boolean.class) {
      return false;
    }
    if (returnType == byte.class) {
      return (byte) 0;
    }
    if (returnType == short.class) {
      return (short) 0;
    }
    if (returnType == int.class) {
      return 0;
    }
    if (returnType == long.class) {
      return 0L;
    }
    if (returnType == float.class) {
      return 0F;
    }
    if (returnType == double.class) {
      return 0D;
    }
    if (returnType == char.class) {
      return '\0';
    }
    return null;
  }
}
