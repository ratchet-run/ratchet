---
sidebar_position: 3
title: Kubernetes Deployment
description: Deploying Ratchet on Kubernetes with Deployments, StatefulSets, ConfigMaps, and health probes.
---

# Kubernetes Deployment

Ratchet runs on Kubernetes as a standard Jakarta EE 10/11 application. This guide covers single-replica Deployments, clustered StatefulSets, ConfigMaps for configuration, and health probe setup.

## Single-node deployment

For non-clustered workloads, a standard Deployment is sufficient:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ratchet-app
  labels:
    app: ratchet-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ratchet-app
  template:
    metadata:
      labels:
        app: ratchet-app
    spec:
      containers:
      - name: app
        image: myapp:latest
        ports:
        - containerPort: 8080
          name: http
        envFrom:
        - configMapRef:
            name: ratchet-config
        - secretRef:
            name: ratchet-db-credentials
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "2"
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 2
          failureThreshold: 1
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 30
        startupProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
          failureThreshold: 30
```

## Clustered StatefulSet

For multi-node Ratchet deployments where each node needs a stable identity (used by `NodeIdentityProvider`), use a StatefulSet:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ratchet-scheduler
spec:
  serviceName: ratchet-scheduler
  replicas: 3
  selector:
    matchLabels:
      app: ratchet-scheduler
  template:
    metadata:
      labels:
        app: ratchet-scheduler
    spec:
      containers:
      - name: app
        image: myapp:latest
        ports:
        - containerPort: 8080
          name: http
        env:
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        envFrom:
        - configMapRef:
            name: ratchet-config
        - secretRef:
            name: ratchet-db-credentials
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "2"
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 2
          failureThreshold: 1
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 30
```

StatefulSet pods get stable names (`ratchet-scheduler-0`, `ratchet-scheduler-1`, etc.) that serve as natural node identifiers. The `metadata.name` field is injected via the Downward API and can be read by a `NodeIdentityProvider` implementation:

There is no `RATCHET_CLUSTER_ENABLED` flag. Multiple replicas become a Ratchet cluster when they share the same store. One-shot claims, recurring scans, and destructive startup cleanup are already coordinated through store-backed locking; bake in a real `ClusterCoordinator` only if you want cross-node wakeups.

```java
@ApplicationScoped
public class KubernetesNodeProvider implements NodeIdentityProvider {

  @Override
  public String getNodeId() {
    String nodeId = System.getenv("NODE_ID");
    return nodeId != null ? nodeId : System.getenv("HOSTNAME");
  }
}
```

## Headless Service

A headless Service is required for StatefulSet DNS resolution:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ratchet-scheduler
  labels:
    app: ratchet-scheduler
spec:
  clusterIP: None
  selector:
    app: ratchet-scheduler
  ports:
  - port: 8080
    name: http
```

This allows pods to discover each other at `ratchet-scheduler-0.ratchet-scheduler.default.svc.cluster.local`.

## External-Facing Service

Expose the application to traffic with a standard Service:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ratchet-app
  labels:
    app: ratchet-scheduler
spec:
  type: ClusterIP
  selector:
    app: ratchet-scheduler
  ports:
  - port: 80
    targetPort: 8080
    name: http
```

## Ingress

Route external traffic to the application:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ratchet-app
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "120"
spec:
  rules:
  - host: ratchet.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: ratchet-app
            port:
              number: 80
```

## ConfigMaps

Store non-sensitive Ratchet configuration in a ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ratchet-config
data:
  RATCHET_THREAD_POOL_SIZE_SINGLE: "16"
  RATCHET_THREAD_POOL_SIZE_RECURRING: "5"
  RATCHET_POLLER_MIN_DELAY_MS: "2000"
  RATCHET_POLLER_MAX_DELAY_MS: "10000"
  RATCHET_POLLER_BATCH_SIZE: "100"
  RATCHET_JOB_RETENTION_DAYS: "30"
  RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS: "10"
```

## Secrets

Store database credentials in a Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: ratchet-db-credentials
type: Opaque
stringData:
  DB_URL: jdbc:postgresql://postgres-service:5432/ratchet
  DB_USERNAME: ratchet
  DB_PASSWORD: your-secure-password
```

In production, use an external secret manager (AWS Secrets Manager, HashiCorp Vault, etc.) with an operator like External Secrets to sync credentials into Kubernetes Secrets.

## Health probes

Ratchet applications should expose MicroProfile Health endpoints. WildFly and other Jakarta EE runtimes serve these automatically when the `microprofile-health` subsystem is enabled.

### Startup probe

The startup probe prevents the readiness and liveness probes from running until the application has fully initialized. Jakarta EE applications can take 30-60 seconds to start, especially when connecting to databases and initializing CDI contexts.

```yaml
startupProbe:
  httpGet:
    path: /health/ready
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 5
  failureThreshold: 30    # 10 + (5 * 30) = up to 160 seconds to start
```

### Readiness probe

The readiness probe controls whether the pod receives request traffic. It should combine an
application-owned traffic-acceptance signal with any dependencies required to serve requests, such
as the Ratchet store. Ratchet does not ship a MicroProfile Health check or a public Poller-status
API.

```yaml
readinessProbe:
  httpGet:
    path: /health/ready
    port: 8080
  periodSeconds: 2
  failureThreshold: 1
```

### Liveness probe

The liveness probe restarts the pod if it becomes unresponsive. Use a longer interval and higher failure threshold to avoid unnecessary restarts during garbage collection pauses or temporary database hiccups:

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 8080
  periodSeconds: 30
  failureThreshold: 5
```

### Custom health check

This example uses a file as the application's traffic-acceptance signal. The path is an application
convention, not a file created or read by Ratchet. Use a writable in-container path (for example,
an `emptyDir` mount) or expose an equivalent application-owned endpoint when the image has a
read-only filesystem:

```java
@Readiness
@ApplicationScoped
public class RatchetReadinessCheck implements HealthCheck {

  private static final Path NOT_READY = Path.of("/tmp/ratchet-not-ready");

  @Resource(lookup = "java:/RatchetDS")
  DataSource dataSource;

  @Override
  public HealthCheckResponse call() {
    if (Files.exists(NOT_READY)) {
      return HealthCheckResponse.down("ratchet-application")
          .withData("reason", "terminating")
          .build();
    }

    try (Connection conn = dataSource.getConnection()) {
      return conn.isValid(2)
          ? HealthCheckResponse.up("ratchet-application").build()
          : HealthCheckResponse.down("ratchet-application")
              .withData("reason", "store validation failed")
              .build();
    } catch (Exception e) {
      return HealthCheckResponse.down("ratchet-application")
          .withData("error", e.getMessage())
          .build();
    }
  }
}
```

### Rolling termination

`DrainController` belongs to the reference implementation, is not exported as a supported
application API, and has no built-in MicroProfile Health adapter. Its state changes as CDI shutdown
begins, so it cannot be the signal that a preStop hook uses to remove request traffic first.

Use a preStop hook to flip the same application-owned signal checked above, then wait long enough
for readiness and any ingress or service-mesh routing state to converge. Kubernetes already marks a
terminating Service endpoint as not ready; the explicit flag keeps the application's own health
state consistent and also works for traffic paths that consult the health endpoint directly. See
the Kubernetes documentation on [readiness probes](https://kubernetes.io/docs/concepts/workloads/pods/probes/#readiness-probe)
and [container lifecycle hooks](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/).

```yaml
spec:
  terminationGracePeriodSeconds: 30
  containers:
  - name: app
    image: myapp:latest
    lifecycle:
      preStop:
        exec:
          command:
          - /bin/sh
          - -c
          - touch /tmp/ratchet-not-ready && sleep 10
    readinessProbe:
      httpGet:
        path: /health/ready
        port: 8080
      periodSeconds: 2
      failureThreshold: 1
```

The file flag does not switch Ratchet's internal drain mode. It only stops new request traffic.
After preStop returns, Kubernetes sends the container its termination signal. The Jakarta runtime
then destroys the application, and Ratchet's lifecycle stops new claims, stops its background
services, and requests cancellation of active executions. Any job left RUNNING is recovered through
the normal node-orphan path, so job code must remain safe for at-least-once execution.

The termination grace period includes the preStop hook. Set it long enough for the routing delay and
normal application-server teardown, but do not use it as a promise that arbitrary job runtimes will
finish before shutdown.

## Database on Kubernetes

### Managed database service (recommended)

For production, use a managed database service (RDS, Cloud SQL, Azure Database) rather than running the database in Kubernetes. Point the `DB_URL` to the managed instance:

```yaml
# In the Secret
stringData:
  DB_URL: jdbc:postgresql://ratchet-db.abc123.us-east-1.rds.amazonaws.com:5432/ratchet
```

### PostgreSQL in Kubernetes

For development or when a managed service is not available:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:15
        ports:
        - containerPort: 5432
        env:
        - name: POSTGRES_DB
          value: ratchet
        - name: POSTGRES_USER
          valueFrom:
            secretKeyRef:
              name: ratchet-db-credentials
              key: DB_USERNAME
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: ratchet-db-credentials
              key: DB_PASSWORD
        volumeMounts:
        - name: pgdata
          mountPath: /var/lib/postgresql/data
        - name: init-scripts
          mountPath: /docker-entrypoint-initdb.d
  volumeClaimTemplates:
  - metadata:
      name: pgdata
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-init
data:
  01-schema.sql: |
    -- Paste contents of postgresql-schema.sql here,
    -- or mount from a volume containing the DDL file
```

## Schema initialization

Apply the Ratchet schema as a Kubernetes Job that runs before the application starts:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: ratchet-schema-init
spec:
  template:
    spec:
      containers:
      - name: schema-init
        image: postgres:15
        command:
        - sh
        - -c
        - |
          until pg_isready -h postgres-service -U ratchet; do
            echo "Waiting for PostgreSQL..."
            sleep 2
          done
          psql -h postgres-service -U ratchet -d ratchet -f /schema/postgresql-schema.sql
        env:
        - name: PGPASSWORD
          valueFrom:
            secretKeyRef:
              name: ratchet-db-credentials
              key: DB_PASSWORD
        volumeMounts:
        - name: schema
          mountPath: /schema
      volumes:
      - name: schema
        configMap:
          name: ratchet-schema
      restartPolicy: OnFailure
  backoffLimit: 5
```

## Resource recommendations

Sizing guidelines based on workload:

| Workload | Replicas | CPU Request | Memory Request | Polling Interval |
|----------|----------|-------------|----------------|-----------------|
| Light (< 100 jobs/hr) | 1 | 250m | 256Mi | 10s |
| Medium (100-1,000 jobs/hr) | 2 | 500m | 512Mi | 5s |
| Heavy (1,000-10,000 jobs/hr) | 3 | 1 | 1Gi | 3s |
| Extreme (> 10,000 jobs/hr) | 5+ | 2 | 2Gi | 1s |

## Pod disruption budget

Prevent Kubernetes from evicting too many Ratchet pods during node maintenance:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ratchet-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: ratchet-scheduler
```

## See also

- [Docker Deployment](/deployment/docker) -- Building container images
- [Cluster Configuration](/deployment/cluster-configuration) -- ClusterCoordinator and node identity
- [Performance Tuning](/deployment/performance-tuning) -- Tuning for high throughput
- [Configuration](/deployment/configuration) -- Full configuration reference
