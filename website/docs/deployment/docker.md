---
sidebar_position: 2
title: Docker Deployment
description: Running Ratchet applications in Docker with WildFly or Payara, including Docker Compose examples.
---

# Docker Deployment

Ratchet runs on any Jakarta EE 10 runtime. This guide shows how to containerize a Ratchet application using WildFly or Payara as the runtime, with Docker Compose for local development.

## Base Image Selection

Choose a base image that provides a Jakarta EE 10 Web Profile runtime:

| Runtime | Base Image | Notes |
|---------|-----------|-------|
| **WildFly** | `quay.io/wildfly/wildfly:39.0.1.Final-jdk17` | Recommended for Ratchet (used in CI) |
| **Payara Micro** | `payara/micro:6.2024.6-jdk17` | Lightweight, good for microservices |
| **Open Liberty** | `icr.io/appcafe/open-liberty:24.0.0.3-full-java17-openj9` | Feature-based configuration |

## WildFly Dockerfile

This Dockerfile builds a WAR deployment on WildFly with a PostgreSQL data source:

```dockerfile
FROM quay.io/wildfly/wildfly:39.0.1.Final-jdk17

# Add the PostgreSQL JDBC driver
ADD --chown=jboss:root \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    /opt/jboss/wildfly/standalone/deployments/postgresql-42.7.4.jar

# Copy WildFly CLI commands for data source setup
COPY wildfly-config.cli /opt/jboss/

# Run CLI configuration
RUN /opt/jboss/wildfly/bin/jboss-cli.sh --file=/opt/jboss/wildfly-config.cli

# Deploy your application WAR
COPY target/myapp.war /opt/jboss/wildfly/standalone/deployments/

# Expose HTTP and management ports
EXPOSE 8080 9990

CMD ["/opt/jboss/wildfly/bin/standalone.sh", "-b", "0.0.0.0"]
```

The `wildfly-config.cli` script configures the data source:

```bash
# wildfly-config.cli
embed-server --server-config=standalone.xml

# Install PostgreSQL driver as a module
module add --name=org.postgresql \
    --resources=/opt/jboss/wildfly/standalone/deployments/postgresql-42.7.4.jar \
    --dependencies=javax.api,javax.transaction.api

# Register the JDBC driver
/subsystem=datasources/jdbc-driver=postgresql:add( \
    driver-name=postgresql, \
    driver-module-name=org.postgresql, \
    driver-class-name=org.postgresql.Driver)

# Create the data source
/subsystem=datasources/data-source=RatchetDS:add( \
    jndi-name=java:/RatchetDS, \
    driver-name=postgresql, \
    connection-url=${env.DB_URL:jdbc:postgresql://localhost:5432/ratchet}, \
    user-name=${env.DB_USERNAME:ratchet}, \
    password=${env.DB_PASSWORD:ratchet}, \
    min-pool-size=5, \
    max-pool-size=20, \
    valid-connection-checker-class-name=org.jboss.jca.adapters.jdbc.extensions.postgres.PostgreSQLValidConnectionChecker)

stop-embedded-server
```

### WildFly with MySQL

For MySQL, swap the driver and data source configuration:

```dockerfile
FROM quay.io/wildfly/wildfly:39.0.1.Final-jdk17

ADD --chown=jboss:root \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar \
    /opt/jboss/wildfly/standalone/deployments/mysql-connector-j-8.3.0.jar

COPY wildfly-mysql-config.cli /opt/jboss/
RUN /opt/jboss/wildfly/bin/jboss-cli.sh --file=/opt/jboss/wildfly-mysql-config.cli

COPY target/myapp.war /opt/jboss/wildfly/standalone/deployments/

EXPOSE 8080
CMD ["/opt/jboss/wildfly/bin/standalone.sh", "-b", "0.0.0.0"]
```

```bash
# wildfly-mysql-config.cli
embed-server --server-config=standalone.xml

module add --name=com.mysql \
    --resources=/opt/jboss/wildfly/standalone/deployments/mysql-connector-j-8.3.0.jar \
    --dependencies=javax.api,javax.transaction.api

/subsystem=datasources/jdbc-driver=mysql:add( \
    driver-name=mysql, \
    driver-module-name=com.mysql, \
    driver-class-name=com.mysql.cj.jdbc.Driver)

/subsystem=datasources/data-source=RatchetDS:add( \
    jndi-name=java:/RatchetDS, \
    driver-name=mysql, \
    connection-url=${env.DB_URL:jdbc:mysql://localhost:3306/ratchet}, \
    user-name=${env.DB_USERNAME:ratchet}, \
    password=${env.DB_PASSWORD:ratchet}, \
    min-pool-size=5, \
    max-pool-size=20, \
    transaction-isolation=TRANSACTION_READ_COMMITTED, \
    valid-connection-checker-class-name=org.jboss.jca.adapters.jdbc.extensions.mysql.MySQLValidConnectionChecker)

stop-embedded-server
```

:::caution MySQL Isolation Level
MySQL defaults to `REPEATABLE READ`, which causes gap locks on `SELECT ... FOR UPDATE` that block concurrent inserts. Always set `transaction-isolation=TRANSACTION_READ_COMMITTED` on the data source, or append `?sessionVariables=transaction_isolation='READ-COMMITTED'` to the JDBC URL.
:::

## Payara Micro Dockerfile

Payara Micro deploys WARs directly without a full application server install:

```dockerfile
FROM payara/micro:6.2024.6-jdk17

# Copy the PostgreSQL driver into the lib directory
COPY --chown=payara:payara postgresql-42.7.4.jar /opt/payara/libs/

# Copy your application WAR
COPY --chown=payara:payara target/myapp.war /opt/payara/deployments/

# Copy post-boot commands for data source configuration
COPY --chown=payara:payara post-boot.txt /opt/payara/

CMD ["--deploymentDir", "/opt/payara/deployments", \
     "--postbootcommandfile", "/opt/payara/post-boot.txt", \
     "--addLibs", "/opt/payara/libs/postgresql-42.7.4.jar"]
```

```bash
# post-boot.txt
create-jdbc-connection-pool --datasourceclassname=org.postgresql.ds.PGSimpleDataSource --restype=javax.sql.DataSource --property=serverName=${ENV=DB_HOST}:portNumber=${ENV=DB_PORT}:databaseName=${ENV=DB_NAME}:user=${ENV=DB_USERNAME}:password=${ENV=DB_PASSWORD} RatchetPool
create-jdbc-resource --connectionpoolid=RatchetPool java:/RatchetDS
```

## Environment Variables

Configure Ratchet via environment variables in your container:

```dockerfile
ENV RATCHET_THREAD_POOL_SIZE_SINGLE=16
ENV RATCHET_POLLER_MIN_DELAY_MS=2000
ENV RATCHET_POLLER_MAX_DELAY_MS=10000
ENV RATCHET_POLLER_BATCH_SIZE=100
ENV RATCHET_JOB_RETENTION_DAYS=30
ENV RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS=10
```

Or pass them at runtime:

```bash
docker run -d \
  -e DB_URL=jdbc:postgresql://db:5432/ratchet \
  -e DB_USERNAME=ratchet \
  -e DB_PASSWORD=secret \
  -e RATCHET_THREAD_POOL_SIZE_SINGLE=32 \
  -e RATCHET_POLLER_MIN_DELAY_MS=1000 \
  -e RATCHET_POLLER_BATCH_SIZE=100 \
  -p 8080:8080 \
  myapp:latest
```

## Docker Compose

### With PostgreSQL

```yaml
services:
  app:
    build: .
    ports:
      - "8080:8080"
    environment:
      DB_URL: jdbc:postgresql://postgres:5432/ratchet
      DB_USERNAME: ratchet
      DB_PASSWORD: ratchet
      RATCHET_THREAD_POOL_SIZE_SINGLE: "16"
      RATCHET_POLLER_MIN_DELAY_MS: "2000"
      RATCHET_POLLER_BATCH_SIZE: "100"
    depends_on:
      postgres:
        condition: service_healthy

  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: ratchet
      POSTGRES_USER: ratchet
      POSTGRES_PASSWORD: ratchet
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./schema/postgresql-schema.sql:/docker-entrypoint-initdb.d/01-schema.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ratchet"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  pgdata:
```

The `postgresql-schema.sql` file is applied automatically by the PostgreSQL container on first startup via the `docker-entrypoint-initdb.d` mechanism.

To extract the DDL from the Ratchet JAR:

```bash
# Extract from the store module JAR
jar xf ratchet-store-postgresql-0.1.0-SNAPSHOT.jar ddl/postgresql-schema.sql
cp ddl/postgresql-schema.sql schema/
```

### With MySQL

```yaml
services:
  app:
    build: .
    ports:
      - "8080:8080"
    environment:
      DB_URL: "jdbc:mysql://mysql:3306/ratchet?sessionVariables=transaction_isolation='READ-COMMITTED'"
      DB_USERNAME: ratchet
      DB_PASSWORD: ratchet
    depends_on:
      mysql:
        condition: service_healthy

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_DATABASE: ratchet
      MYSQL_USER: ratchet
      MYSQL_PASSWORD: ratchet
      MYSQL_ROOT_PASSWORD: rootpassword
    ports:
      - "3306:3306"
    volumes:
      - mysqldata:/var/lib/mysql
      - ./schema/mysql-schema.sql:/docker-entrypoint-initdb.d/01-schema.sql
    command: >
      --default-authentication-plugin=caching_sha2_password
      --character-set-server=utf8mb4
      --collation-server=utf8mb4_unicode_ci
      --transaction-isolation=READ-COMMITTED
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      timeout: 5s
      retries: 10

volumes:
  mysqldata:
```

### Multi-Node Cluster

For testing clustered deployments locally:

```yaml
services:
  app1:
    build: .
    hostname: ratchet-node-1
    ports:
      - "8081:8080"
    environment:
      DB_URL: jdbc:postgresql://postgres:5432/ratchet
      DB_USERNAME: ratchet
      DB_PASSWORD: ratchet
      RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS: "10"
    depends_on:
      postgres:
        condition: service_healthy

  app2:
    build: .
    hostname: ratchet-node-2
    ports:
      - "8082:8080"
    environment:
      DB_URL: jdbc:postgresql://postgres:5432/ratchet
      DB_USERNAME: ratchet
      DB_PASSWORD: ratchet
      RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS: "10"
    depends_on:
      postgres:
        condition: service_healthy

  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: ratchet
      POSTGRES_USER: ratchet
      POSTGRES_PASSWORD: ratchet
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./schema/postgresql-schema.sql:/docker-entrypoint-initdb.d/01-schema.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ratchet"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  pgdata:
```

There is no `RATCHET_CLUSTER_ENABLED` switch. Sharing the same store across multiple Ratchet nodes makes the deployment multi-node automatically. One-shot job claiming, recurring-scheduler singleton execution, and destructive startup cleanup are already coordinated through the store. Add a real `ClusterCoordinator` only if you want cross-node wakeups.

## Volume Configuration

### Schema Files

Mount your DDL files into the database container's init directory:

```yaml
volumes:
  - ./schema/postgresql-schema.sql:/docker-entrypoint-initdb.d/01-schema.sql
```

These run automatically on first container creation. For subsequent schema updates, use your migration tool or apply manually:

```bash
docker exec -i my-postgres psql -U ratchet -d ratchet < schema/postgresql-schema.sql
```

### Persistent Storage

Always use named volumes for database data to survive container restarts:

```yaml
volumes:
  pgdata:
    driver: local
```

## Health Checks

Add a health check endpoint to your application and configure Docker to use it:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/health/ready || exit 1
```

WildFly exposes MicroProfile Health at `/health/ready` and `/health/live` when the `microprofile-health` subsystem is enabled.

## Building the Image

```bash
# Build the application
mvn clean package -DskipTests

# Build the Docker image
docker build -t myapp:latest .

# Run with Docker Compose
docker compose up -d

# Check logs
docker compose logs -f app

# Verify schema was applied
docker compose exec postgres psql -U ratchet -c "\dt scheduler_*"
```

## See Also

- [Kubernetes Deployment](/docs/deployment/kubernetes) — Orchestrated container deployments
- [Database Setup](/docs/deployment/database-setup) — Schema application details
- [Configuration](/docs/deployment/configuration) — All Ratchet configuration properties
- [Cluster Configuration](/docs/deployment/cluster-configuration) — Multi-node coordination
