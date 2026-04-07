---
sidebar_position: 3
title: Kubernetes Deployment
description: Deploying Ratchet on Kubernetes with Deployments, StatefulSets, ConfigMaps, and health probes.
---

# Kubernetes Deployment

Ratchet runs on Kubernetes as a standard Jakarta EE application. This guide covers single-replica Deployments, clustered StatefulSets, ConfigMaps for configuration, and health probe setup.

## Single-Node Deployment

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
          periodSeconds: 10
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
        - name: RATCHET_CLUSTER_ENABLED
          value: "true"
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
          periodSeconds: 10
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 30
```

StatefulSet pods get stable names (`ratchet-scheduler-0`, `ratchet-scheduler-1`, etc.) that serve as natural node identifiers. The `metadata.name` field is injected via the Downward API and can be read by a `NodeIdentityProvider` implementation:

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
  RATCHET_EXECUTOR_THREADS: "16"
  RATCHET_POLLING_INTERVAL: "5000"
  RATCHET_POLLING_BATCH_SIZE: "100"
  RATCHET_POLLING_ADAPTIVE: "true"
  RATCHET_RETENTION_COMPLETED_DAYS: "30"
  RATCHET_CLUSTER_ENABLED: "true"
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

## Health Probes

Ratchet applications should expose MicroProfile Health endpoints. WildFly and other Jakarta EE runtimes serve these automatically when the `microprofile-health` subsystem is enabled.

### Startup Probe

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

### Readiness Probe

The readiness probe controls whether the pod receives traffic. It should verify that the Ratchet polling engine is running and the database is reachable:

```yaml
readinessProbe:
  httpGet:
    path: /health/ready
    port: 8080
  periodSeconds: 10
  failureThreshold: 3
```

### Liveness Probe

The liveness probe restarts the pod if it becomes unresponsive. Use a longer interval and higher failure threshold to avoid unnecessary restarts during garbage collection pauses or temporary database hiccups:

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 8080
  periodSeconds: 30
  failureThreshold: 5
```

### Custom Health Check

Implement a Ratchet-specific health check:

```java
@Readiness
@ApplicationScoped
public class RatchetReadinessCheck implements HealthCheck {

  @Inject
  JobSchedulerService scheduler;

  @Override
  public HealthCheckResponse call() {
    try {
      // Verify store connectivity by running a minimal query
      scheduler.findJobs(new JobQuery().limit(1));
      return HealthCheckResponse.up("ratchet-store").build();
    } catch (Exception e) {
      return HealthCheckResponse.down("ratchet-store")
          .withData("error", e.getMessage())
          .build();
    }
  }
}
```

## Database on Kubernetes

### Managed Database Service (Recommended)

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

## Schema Initialization

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

## Resource Recommendations

Sizing guidelines based on workload:

| Workload | Replicas | CPU Request | Memory Request | Polling Interval |
|----------|----------|-------------|----------------|-----------------|
| Light (< 100 jobs/hr) | 1 | 250m | 256Mi | 10s |
| Medium (100-1,000 jobs/hr) | 2 | 500m | 512Mi | 5s |
| Heavy (1,000-10,000 jobs/hr) | 3 | 1 | 1Gi | 3s |
| Extreme (> 10,000 jobs/hr) | 5+ | 2 | 2Gi | 1s |

## Pod Disruption Budget

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

## See Also

- [Docker Deployment](/docs/deployment/docker) — Building container images
- [Cluster Configuration](/docs/deployment/cluster-configuration) — ClusterCoordinator and node identity
- [Performance Tuning](/docs/deployment/performance-tuning) — Tuning for high throughput
- [Configuration](/docs/deployment/configuration) — Full configuration reference
