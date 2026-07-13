#!/usr/bin/env bash
set -euo pipefail

server="${SHOWCASE_SERVER:-unconfigured}"
db="${SHOWCASE_DB:-postgresql}"
build_dir="${SHOWCASE_BUILD_DIR:?SHOWCASE_BUILD_DIR is required}"
war="${SHOWCASE_WAR:?SHOWCASE_WAR is required}"
context="${SHOWCASE_CONTEXT_PATH:-/}"
if [[ -z "$context" ]]; then
  context="/"
elif [[ "$context" != /* ]]; then
  context="/$context"
fi
if [[ "$context" != "/" ]]; then
  context="${context%/}"
fi

if [[ ! -f "$war" ]]; then
  echo "WAR not found: $war" >&2
  exit 1
fi

export SHOWCASE_SERVER="$server"
export SHOWCASE_DB="$db"
export SHOWCASE_VERSION="${SHOWCASE_VERSION:-dev}"
export RATCHET_SCHEMA_AUTO_MIGRATE="${RATCHET_SCHEMA_AUTO_MIGRATE:-false}"
export RATCHET_SCHEMA_MIGRATION_DIALECT="${RATCHET_SCHEMA_MIGRATION_DIALECT:-$db}"
export RATCHET_RECURRING_STARTUP_GRACE_SECONDS="${RATCHET_RECURRING_STARTUP_GRACE_SECONDS:-0}"
export RATCHET_NODE_ID="${RATCHET_NODE_ID:-showcase-${server}-${db}-$$}"
export TZ="${TZ:-UTC}"

# An explicitly supplied connection target means "use my external database" and
# turns off the embedded container, whatever SHOWCASE_DB_EMBEDDED says. We record
# that before applying defaults below, so the unset case can be served by a
# container we start ourselves.
embedded_db="${SHOWCASE_DB_EMBEDDED:-true}"
external_db_configured=false
case "$db" in
  postgresql) [[ -n "${POSTGRES_HOST:-}" ]] && external_db_configured=true ;;
  mysql) [[ -n "${MYSQL_HOST:-}" ]] && external_db_configured=true ;;
  oracle) [[ -n "${ORACLE_HOST:-}" ]] && external_db_configured=true ;;
  sqlserver) [[ -n "${SQLSERVER_HOST:-}" ]] && external_db_configured=true ;;
  mongodb) [[ -n "${MONGO_URI:-}" ]] && external_db_configured=true ;;
esac

postgres_host="${POSTGRES_HOST:-localhost}"
postgres_port="${POSTGRES_PORT:-5432}"
postgres_db="${POSTGRES_DB:-ratchet}"
postgres_user="${POSTGRES_USER:-ratchet}"
postgres_password="${POSTGRES_PASSWORD:-ratchet}"

mysql_host="${MYSQL_HOST:-localhost}"
mysql_port="${MYSQL_PORT:-3306}"
mysql_db="${MYSQL_DATABASE:-ratchet}"
mysql_user="${MYSQL_USER:-ratchet}"
mysql_password="${MYSQL_PASSWORD:-ratchet}"

oracle_host="${ORACLE_HOST:-localhost}"
oracle_port="${ORACLE_PORT:-1521}"
oracle_service="${ORACLE_SERVICE:-FREEPDB1}"
oracle_user="${ORACLE_USER:-ratchet}"
oracle_password="${ORACLE_PASSWORD:-ratchet}"

sqlserver_host="${SQLSERVER_HOST:-localhost}"
sqlserver_port="${SQLSERVER_PORT:-1433}"
sqlserver_db="${SQLSERVER_DATABASE:-ratchet}"
sqlserver_user="${SQLSERVER_USER:-ratchet}"
# SQL Server enforces SA password complexity; the embedded path below reuses this.
sqlserver_password="${SQLSERVER_PASSWORD:-Ratchet!Str0ngPwd}"

export MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
export MONGO_DATABASE="${MONGO_DATABASE:-ratchet}"

jdbc_dir="$build_dir/showcase-jdbc"
ds_jndi="${SHOWCASE_DATASOURCE_JNDI:-java:jboss/datasources/RatchetDS}"

# --- Embedded database container -------------------------------------------
# When no external database is configured, the launcher starts a throwaway
# container so the whole demo is one Maven command with Docker as the only
# prerequisite. The container publishes on a random loopback port (so a local
# Postgres/MySQL/Mongo on the standard port doesn't collide) and is removed on
# exit. Set SHOWCASE_DB_EMBEDDED=false, or point POSTGRES_HOST/MYSQL_HOST/
# ORACLE_HOST/SQLSERVER_HOST/MONGO_URI at your own database, to opt out.
db_container_id=""

docker_available() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

cleanup_db_container() {
  if [[ -n "$db_container_id" ]]; then
    docker rm -f "$db_container_id" >/dev/null 2>&1 || true
    db_container_id=""
  fi
}

# Maps a container's internal port to the host port Docker assigned it.
published_port() {
  docker port "$db_container_id" "$1/tcp" 2>/dev/null | head -n 1 | awk -F: '{print $NF}'
}

wait_for_db_ready() {
  local probe=("$@")
  local deadline=$((SECONDS + 60))
  until docker exec "$db_container_id" "${probe[@]}" >/dev/null 2>&1; do
    if ! docker ps -q --no-trunc | grep -q "$db_container_id"; then
      echo "Embedded database container exited before becoming ready." >&2
      docker logs "$db_container_id" 2>&1 | tail -n 20 >&2 || true
      exit 1
    fi
    if ((SECONDS >= deadline)); then
      echo "Embedded database did not become ready within 60s." >&2
      exit 1
    fi
    sleep 1
  done
}

start_embedded_db() {
  if ! docker_available; then
    echo "Docker is required to start the embedded showcase database, but the Docker daemon is not reachable." >&2
    echo "Start Docker and retry, set SHOWCASE_DB_EMBEDDED=false, or point POSTGRES_HOST/MYSQL_HOST/ORACLE_HOST/SQLSERVER_HOST/MONGO_URI at an external database." >&2
    exit 1
  fi

  local name="ratchet-showcase-db-$$"
  echo "Starting embedded $db database (Docker)..."
  case "$db" in
    postgresql)
      db_container_id="$(docker run -d --rm --name "$name" \
        -e POSTGRES_USER="$postgres_user" \
        -e POSTGRES_PASSWORD="$postgres_password" \
        -e POSTGRES_DB="$postgres_db" \
        -p 127.0.0.1::5432 postgres:16)"
      wait_for_db_ready pg_isready -U "$postgres_user" -d "$postgres_db"
      postgres_host="127.0.0.1"
      postgres_port="$(published_port 5432)"
      ;;
    mysql)
      db_container_id="$(docker run -d --rm --name "$name" \
        -e MYSQL_ROOT_PASSWORD="root" \
        -e MYSQL_DATABASE="$mysql_db" \
        -e MYSQL_USER="$mysql_user" \
        -e MYSQL_PASSWORD="$mysql_password" \
        -p 127.0.0.1::3306 mysql:8)"
      # Probe over TCP as the application user, not `mysqladmin ping`: mysql:8
      # runs a socket-only temp server during init that answers ping before the
      # real server, user, and database exist, which would let migration race in
      # and fail with "Access denied".
      wait_for_db_ready mysql -h127.0.0.1 -u"$mysql_user" -p"$mysql_password" "$mysql_db" -e "SELECT 1"
      mysql_host="127.0.0.1"
      mysql_port="$(published_port 3306)"
      ;;
    oracle)
      # VERIFY: gvenzl/oracle-free is a multi-hundred-MB image; on a cold pull it can
      # take well over the 60s readiness deadline used by wait_for_db_ready. healthcheck.sh
      # is the image's built-in probe; it also auto-creates APP_USER in FREEPDB1.
      db_container_id="$(docker run -d --rm --name "$name" \
        -e ORACLE_PASSWORD="$oracle_password" \
        -e APP_USER="$oracle_user" \
        -e APP_USER_PASSWORD="$oracle_password" \
        -p 127.0.0.1::1521 gvenzl/oracle-free:23-slim)"
      wait_for_db_ready healthcheck.sh
      oracle_host="127.0.0.1"
      oracle_port="$(published_port 1521)"
      ;;
    sqlserver)
      db_container_id="$(docker run -d --rm --name "$name" \
        -e ACCEPT_EULA=Y \
        -e MSSQL_SA_PASSWORD="$sqlserver_password" \
        -p 127.0.0.1::1433 mcr.microsoft.com/mssql/server:2022-latest)"
      # SA accepts logins only after the engine finishes starting; probe with sqlcmd
      # (the 2022 image ships it at /opt/mssql-tools18/bin; -C trusts the self-signed cert).
      wait_for_db_ready /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$sqlserver_password" -C -Q "SELECT 1"
      # VERIFY: Ratchet's claim path requires READ_COMMITTED_SNAPSHOT, which cannot be set
      # on master, so the app database is created with RCSI before migration runs. The
      # embedded demo connects as sa; an external SQL Server uses SQLSERVER_USER instead.
      docker exec "$db_container_id" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$sqlserver_password" -C -Q \
        "IF DB_ID('$sqlserver_db') IS NULL CREATE DATABASE [$sqlserver_db]; ALTER DATABASE [$sqlserver_db] SET READ_COMMITTED_SNAPSHOT ON; ALTER DATABASE [$sqlserver_db] SET ALLOW_SNAPSHOT_ISOLATION ON;"
      sqlserver_user="sa"
      sqlserver_host="127.0.0.1"
      sqlserver_port="$(published_port 1433)"
      ;;
    mongodb)
      db_container_id="$(docker run -d --rm --name "$name" \
        -p 127.0.0.1::27017 mongo:7)"
      wait_for_db_ready mongosh --quiet --eval "db.adminCommand({ping:1})"
      export MONGO_URI="mongodb://127.0.0.1:$(published_port 27017)"
      ;;
    *)
      echo "Embedded database is not supported for SHOWCASE_DB=$db." >&2
      exit 1
      ;;
  esac
  echo "Embedded $db ready (container ${db_container_id:0:12})."
}

maybe_start_embedded_db() {
  if [[ "$external_db_configured" == "true" ]]; then
    return
  fi
  if [[ "$embedded_db" != "true" ]]; then
    return
  fi
  # Arm cleanup before starting, so a Ctrl-C during the readiness wait still
  # removes the container. cleanup_db_container is a no-op until the run
  # succeeds and sets db_container_id.
  trap cleanup_db_container EXIT
  trap 'cleanup_db_container; exit 130' INT
  trap 'cleanup_db_container; exit 143' TERM
  start_embedded_db
}

java_major() {
  local java_bin="${JAVA_HOME:+$JAVA_HOME/bin/java}"
  if [[ -z "$java_bin" || ! -x "$java_bin" ]]; then
    java_bin="$(command -v java || true)"
  fi
  if [[ -z "$java_bin" ]]; then
    return 0
  fi
  "$java_bin" -version 2>&1 | awk -F '[\".]' '/version/ { print ($2 == "1" ? $3 : $2); exit }'
}

select_server_java() {
  local requested="${SHOWCASE_JAVA_HOME:-}"
  if [[ -n "$requested" ]]; then
    if [[ ! -x "$requested/bin/java" ]]; then
      echo "SHOWCASE_JAVA_HOME does not contain an executable bin/java: $requested" >&2
      exit 1
    fi
    export JAVA_HOME="$requested"
    export PATH="$JAVA_HOME/bin:$PATH"
  elif [[ -x /usr/libexec/java_home ]]; then
    local major
    major="$(java_major || true)"
    if [[ -n "$major" && "$major" -gt 21 && -x /usr/libexec/java_home ]]; then
      local jdk21
      jdk21="$(/usr/libexec/java_home -v 21 2>/dev/null || true)"
      if [[ -n "$jdk21" ]]; then
        export JAVA_HOME="$jdk21"
        export PATH="$JAVA_HOME/bin:$PATH"
      fi
    fi
  fi

  if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
    echo "Server Java: $("$JAVA_HOME/bin/java" -version 2>&1 | head -n 1) ($JAVA_HOME)"
  fi
}

postgres_jar() {
  ls "$jdbc_dir"/postgresql-*.jar 2>/dev/null | head -n 1
}

mysql_jar() {
  ls "$jdbc_dir"/mysql-connector-j-*.jar 2>/dev/null | head -n 1
}

oracle_jar() {
  ls "$jdbc_dir"/ojdbc11-*.jar 2>/dev/null | head -n 1
}

sqlserver_jar() {
  ls "$jdbc_dir"/mssql-jdbc-*.jar 2>/dev/null | head -n 1
}

ensure_sql_driver_available() {
  if [[ "$db" == "postgresql" && -z "$(postgres_jar)" ]]; then
    echo "PostgreSQL driver missing under $jdbc_dir. Run Maven package first." >&2
    exit 1
  fi
  if [[ "$db" == "mysql" && -z "$(mysql_jar)" ]]; then
    echo "MySQL driver missing under $jdbc_dir. Run Maven package first." >&2
    exit 1
  fi
  if [[ "$db" == "oracle" && -z "$(oracle_jar)" ]]; then
    echo "Oracle driver missing under $jdbc_dir. Run Maven package first." >&2
    exit 1
  fi
  if [[ "$db" == "sqlserver" && -z "$(sqlserver_jar)" ]]; then
    echo "SQL Server driver missing under $jdbc_dir. Run Maven package first." >&2
    exit 1
  fi
}

jdbc_url() {
  case "$db" in
    postgresql) echo "jdbc:postgresql://$postgres_host:$postgres_port/$postgres_db" ;;
    mysql) echo "jdbc:mysql://$mysql_host:$mysql_port/$mysql_db" ;;
    oracle) echo "jdbc:oracle:thin:@//$oracle_host:$oracle_port/$oracle_service" ;;
    sqlserver) echo "jdbc:sqlserver://$sqlserver_host:$sqlserver_port;databaseName=$sqlserver_db;encrypt=true;trustServerCertificate=true" ;;
  esac
}

jdbc_user() {
  case "$db" in
    postgresql) echo "$postgres_user" ;;
    mysql) echo "$mysql_user" ;;
    oracle) echo "$oracle_user" ;;
    sqlserver) echo "$sqlserver_user" ;;
  esac
}

jdbc_password() {
  case "$db" in
    postgresql) echo "$postgres_password" ;;
    mysql) echo "$mysql_password" ;;
    oracle) echo "$oracle_password" ;;
    sqlserver) echo "$sqlserver_password" ;;
  esac
}

run_sql_schema_migration() {
  if [[ "$db" == "mongodb" ]]; then
    return
  fi
  ensure_sql_driver_available

  local exploded="$build_dir/ratchet-showcase"
  if [[ ! -d "$exploded/WEB-INF/classes" || ! -d "$exploded/WEB-INF/lib" ]]; then
    echo "Exploded WAR not found under $exploded. Run Maven package first." >&2
    exit 1
  fi

  local java_bin="${JAVA_HOME:+$JAVA_HOME/bin/java}"
  if [[ -z "$java_bin" || ! -x "$java_bin" ]]; then
    java_bin="$(command -v java || true)"
  fi
  if [[ -z "$java_bin" ]]; then
    echo "No Java runtime available for Ratchet schema migration." >&2
    exit 1
  fi

  echo "Applying Ratchet schema migrations for $db"
  "$java_bin" \
    -cp "$exploded/WEB-INF/classes:$exploded/WEB-INF/lib/*:$jdbc_dir/*" \
    run.ratchet.showcase.config.ShowcaseSchemaMigrator \
    "$db" \
    "$(jdbc_url)" \
    "$(jdbc_user)" \
    "$(jdbc_password)"
}

wait_for_port_release() {
  local port="$1"
  local deadline=$((SECONDS + 20))
  while nc -z 127.0.0.1 "$port" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "Port $port is still in use after waiting for WildFly bootstrap shutdown." >&2
      exit 1
    fi
    sleep 0.2
  done
}

wait_for_wildfly_management() {
  local home="$1"
  local management_port="$2"
  local server_pid="$3"
  local deadline=$((SECONDS + 60))
  while ! "$home/bin/jboss-cli.sh" \
    --connect \
    --controller=127.0.0.1:"$management_port" \
    --command=":read-attribute(name=server-state)" >/dev/null 2>&1; do
    if ! kill -0 "$server_pid" >/dev/null 2>&1; then
      wait "$server_pid"
      exit $?
    fi
    if ((SECONDS >= deadline)); then
      echo "WildFly management port $management_port did not become ready." >&2
      exit 1
    fi
    sleep 0.5
  done
}

wait_for_showcase_http() {
  local port="$1"
  local context_path="$2"
  local deadline=$((SECONDS + 60))
  local prefix="$context_path"
  if [[ "$prefix" == "/" ]]; then
    prefix=""
  fi
  local url="http://127.0.0.1:$port$prefix/api/runtime"
  while ! curl -fsS "$url" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "Ratchet Showcase did not become ready at $url." >&2
      exit 1
    fi
    sleep 0.5
  done
}

run_showcase_smoke() {
  local port="$1"
  local base_url
  base_url="$(showcase_url "$port")"
  base_url="${base_url%/}"

  local runtime
  runtime="$(curl -fsS "$base_url/api/runtime")"
  if ! grep -Eq '"serverProfile"[[:space:]]*:[[:space:]]*"wildfly"' <<<"$runtime"; then
    echo "Showcase smoke expected the WildFly runtime profile: $runtime" >&2
    return 1
  fi
  if ! grep -Eq '"dbProfile"[[:space:]]*:[[:space:]]*"postgresql"' <<<"$runtime"; then
    echo "Showcase smoke expected the PostgreSQL database profile: $runtime" >&2
    return 1
  fi

  curl -fsS "$base_url/" >/dev/null

  local submission
  submission="$(
    curl -fsS \
      -H 'Content-Type: application/json' \
      -d '{"count":3,"seed":12345}' \
      "$base_url/api/scenarios/import-burst"
  )"
  if ! grep -Eq '"batchJobId"[[:space:]]*:[[:space:]]*"[^"]+"' <<<"$submission"; then
    echo "Showcase smoke did not receive a batch job id: $submission" >&2
    return 1
  fi

  local dashboard=""
  local observed_job=false
  for _ in {1..60}; do
    # The curl must stay inside the condition: a bare assignment under `set -e`
    # aborts the script on the first transient failure instead of retrying.
    if dashboard="$(curl -fsS "$base_url/api/dashboard")" \
      && grep -Eq '"recentJobs"[[:space:]]*:[[:space:]]*\[[[:space:]]*\{' <<<"$dashboard"; then
      observed_job=true
      break
    fi
    sleep 0.5
  done
  if [[ "$observed_job" != "true" ]]; then
    echo "Showcase smoke submitted work but the live dashboard never observed a job: $dashboard" >&2
    return 1
  fi

  local metrics
  metrics="$(curl -fsS "$base_url/metrics")"
  if ! grep -q '^ratchet_showcase_orders' <<<"$metrics"; then
    echo "Showcase smoke did not observe showcase metrics." >&2
    return 1
  fi

  echo "Ratchet Showcase smoke passed: WildFly/PostgreSQL boot, API submission, job visibility, and metrics."
}

prepare_wildfly_run_home() {
  local source_home="$1"
  local port="$2"
  local management_port="$3"
  local run_home="$build_dir/wildfly-runs/$port-$management_port"

  mkdir -p "$(dirname "$run_home")"
  if command -v rsync >/dev/null 2>&1; then
    rsync -a --delete \
      --exclude='/standalone/data/' \
      --exclude='/standalone/deployments/' \
      --exclude='/standalone/log/' \
      --exclude='/standalone/tmp/' \
      "$source_home/" "$run_home/"
  else
    rm -rf "$run_home"
    cp -R "$source_home" "$run_home"
    rm -rf "$run_home/standalone/data" \
      "$run_home/standalone/deployments" \
      "$run_home/standalone/log" \
      "$run_home/standalone/tmp"
  fi

  mkdir -p "$run_home/standalone/data" \
    "$run_home/standalone/deployments" \
    "$run_home/standalone/log" \
    "$run_home/standalone/tmp"
  echo "$run_home"
}

shutdown_wildfly() {
  local home="$1"
  local management_port="$2"
  local server_pid="$3"
  if kill -0 "$server_pid" >/dev/null 2>&1; then
    "$home/bin/jboss-cli.sh" \
      --connect \
      --controller=127.0.0.1:"$management_port" \
      --command=":shutdown" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
  fi
}

wildfly_cleanup_home=""
wildfly_cleanup_management_port=""
wildfly_cleanup_server_pid=""

cleanup_wildfly() {
  if [[ -n "$wildfly_cleanup_home" && -n "$wildfly_cleanup_management_port" && -n "$wildfly_cleanup_server_pid" ]]; then
    shutdown_wildfly "$wildfly_cleanup_home" "$wildfly_cleanup_management_port" "$wildfly_cleanup_server_pid"
  fi
  cleanup_db_container
}

cleanup_wildfly_int() {
  cleanup_wildfly
  exit 130
}

cleanup_wildfly_term() {
  cleanup_wildfly
  exit 143
}

configure_wildfly_modules() {
  local home="$1"
  mkdir -p "$home/modules/org/postgresql/main" "$home/modules/com/mysql/main" \
    "$home/modules/com/oracle/main" "$home/modules/com/microsoft/sqlserver/main"
  if [[ -n "$(postgres_jar)" ]]; then
    cp "$(postgres_jar)" "$home/modules/org/postgresql/main/"
    cat > "$home/modules/org/postgresql/main/module.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<module xmlns="urn:jboss:module:1.9" name="org.postgresql">
  <resources>
    <resource-root path="$(basename "$(postgres_jar)")"/>
  </resources>
  <dependencies>
    <module name="java.sql"/>
    <module name="java.logging"/>
    <module name="java.management"/>
    <module name="jakarta.transaction.api"/>
  </dependencies>
</module>
EOF
  fi
  if [[ -n "$(mysql_jar)" ]]; then
    cp "$(mysql_jar)" "$home/modules/com/mysql/main/"
    cat > "$home/modules/com/mysql/main/module.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<module xmlns="urn:jboss:module:1.9" name="com.mysql">
  <resources>
    <resource-root path="$(basename "$(mysql_jar)")"/>
  </resources>
  <dependencies>
    <module name="java.sql"/>
    <module name="java.naming"/>
    <module name="java.logging"/>
    <module name="java.security.sasl"/>
    <module name="java.management"/>
    <module name="java.xml"/>
    <module name="jakarta.transaction.api"/>
  </dependencies>
</module>
EOF
  fi
  if [[ -n "$(oracle_jar)" ]]; then
    cp "$(oracle_jar)" "$home/modules/com/oracle/main/"
    cat > "$home/modules/com/oracle/main/module.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<module xmlns="urn:jboss:module:1.9" name="com.oracle">
  <resources>
    <resource-root path="$(basename "$(oracle_jar)")"/>
  </resources>
  <dependencies>
    <module name="java.sql"/>
    <module name="java.naming"/>
    <module name="java.logging"/>
    <module name="java.management"/>
    <module name="jakarta.transaction.api"/>
  </dependencies>
</module>
EOF
  fi
  if [[ -n "$(sqlserver_jar)" ]]; then
    cp "$(sqlserver_jar)" "$home/modules/com/microsoft/sqlserver/main/"
    cat > "$home/modules/com/microsoft/sqlserver/main/module.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<module xmlns="urn:jboss:module:1.9" name="com.microsoft.sqlserver">
  <resources>
    <resource-root path="$(basename "$(sqlserver_jar)")"/>
  </resources>
  <dependencies>
    <module name="java.sql"/>
    <module name="java.naming"/>
    <module name="java.logging"/>
    <module name="java.management"/>
    <module name="java.security.jgss"/>
    <module name="java.xml"/>
    <module name="jakarta.transaction.api"/>
  </dependencies>
</module>
EOF
  fi
}

clear_wildfly_deployment_config() {
  local home="$1"
  local cli="$build_dir/showcase-wildfly-clear-deployment.cli"
  cat > "$cli" <<EOF
embed-server --server-config=standalone.xml --std-out=echo
if (outcome == success) of /deployment=ratchet-showcase.war:read-resource
  /deployment=ratchet-showcase.war:remove()
end-if
if (outcome == success) of /deployment=ROOT.war:read-resource
  /deployment=ROOT.war:remove()
end-if
if (outcome == success) of /deployment=app.war:read-resource
  /deployment=app.war:remove()
end-if
stop-embedded-server
EOF
  "$home/bin/jboss-cli.sh" --file="$cli" || true
  perl -0pi -e 's/\n\s*<deployments>.*?<\/deployments>\n/\n/s' \
    "$home/standalone/configuration/standalone.xml"
}

wildfly_runtime_name() {
  local context_root="${context#/}"
  if [[ "$context" == "/" || -z "$context_root" ]]; then
    echo "ROOT.war"
  else
    echo "${context_root}.war"
  fi
}

showcase_url() {
  local port="$1"
  local prefix="$context"
  if [[ "$prefix" == "/" ]]; then
    prefix=""
  fi
  echo "http://localhost:$port$prefix/"
}

server_context_root() {
  if [[ "$context" == "/" ]]; then
    echo "/"
  else
    echo "${context#/}"
  fi
}

run_wildfly() {
  local source_home="${WILDFLY_HOME:?WILDFLY_HOME is required for wildfly-managed}"
  local port="${SHOWCASE_HTTP_PORT:-8080}"
  local management_port="${WILDFLY_MANAGEMENT_PORT:-9990}"
  local https_port="${WILDFLY_HTTPS_PORT:-18443}"
  local runtime_name
  runtime_name="$(wildfly_runtime_name)"
  local home
  home="$(prepare_wildfly_run_home "$source_home" "$port" "$management_port")"
  select_server_java
  chmod +x "$home/bin/jboss-cli.sh" "$home/bin/standalone.sh"
  mkdir -p "$home/standalone/configuration/standalone_xml_history"
  if [[ "$db" != "mongodb" ]]; then
    ensure_sql_driver_available
    run_sql_schema_migration
  fi
  configure_wildfly_modules "$home"
  clear_wildfly_deployment_config "$home"
  if [[ "$db" != "mongodb" ]]; then
    local cli="$build_dir/showcase-wildfly-datasource.cli"
    case "$db" in
      postgresql)
        cat > "$cli" <<EOF
embed-server --server-config=standalone.xml --std-out=echo
if (outcome != success) of /subsystem=datasources/jdbc-driver=postgresql:read-resource
  /subsystem=datasources/jdbc-driver=postgresql:add(driver-name=postgresql,driver-module-name=org.postgresql,driver-class-name=org.postgresql.Driver)
end-if
if (outcome != success) of /subsystem=datasources/data-source=RatchetDS:read-resource
  data-source add --name=RatchetDS --jndi-name=$ds_jndi --driver-name=postgresql --connection-url=$(jdbc_url) --user-name=$(jdbc_user) --password=$(jdbc_password) --enabled=true
end-if
stop-embedded-server
EOF
        ;;
      mysql)
        cat > "$cli" <<EOF
embed-server --server-config=standalone.xml --std-out=echo
if (outcome != success) of /subsystem=datasources/jdbc-driver=mysql:read-resource
  /subsystem=datasources/jdbc-driver=mysql:add(driver-name=mysql,driver-module-name=com.mysql,driver-class-name=com.mysql.cj.jdbc.Driver)
end-if
if (outcome != success) of /subsystem=datasources/data-source=RatchetDS:read-resource
  data-source add --name=RatchetDS --jndi-name=$ds_jndi --driver-name=mysql --connection-url=$(jdbc_url) --user-name=$(jdbc_user) --password=$(jdbc_password) --enabled=true
end-if
stop-embedded-server
EOF
        ;;
      oracle)
        cat > "$cli" <<EOF
embed-server --server-config=standalone.xml --std-out=echo
if (outcome != success) of /subsystem=datasources/jdbc-driver=oracle:read-resource
  /subsystem=datasources/jdbc-driver=oracle:add(driver-name=oracle,driver-module-name=com.oracle,driver-class-name=oracle.jdbc.OracleDriver)
end-if
if (outcome != success) of /subsystem=datasources/data-source=RatchetDS:read-resource
  data-source add --name=RatchetDS --jndi-name=$ds_jndi --driver-name=oracle --connection-url=$(jdbc_url) --user-name=$(jdbc_user) --password=$(jdbc_password) --enabled=true
end-if
stop-embedded-server
EOF
        ;;
      sqlserver)
        # VERIFY: the SQL Server connection URL contains ';' separators. jboss-cli treats
        # ';' as a command separator on a CLI line, but inside a --file script each line is
        # one command, so the unquoted URL should pass through; confirm the datasource adds
        # cleanly and quote the value if the CLI splits it.
        cat > "$cli" <<EOF
embed-server --server-config=standalone.xml --std-out=echo
if (outcome != success) of /subsystem=datasources/jdbc-driver=sqlserver:read-resource
  /subsystem=datasources/jdbc-driver=sqlserver:add(driver-name=sqlserver,driver-module-name=com.microsoft.sqlserver,driver-class-name=com.microsoft.sqlserver.jdbc.SQLServerDriver)
end-if
if (outcome != success) of /subsystem=datasources/data-source=RatchetDS:read-resource
  data-source add --name=RatchetDS --jndi-name=$ds_jndi --driver-name=sqlserver --connection-url=$(jdbc_url) --user-name=$(jdbc_user) --password=$(jdbc_password) --transaction-isolation=TRANSACTION_READ_COMMITTED --enabled=true
end-if
stop-embedded-server
EOF
        ;;
    esac
    "$home/bin/jboss-cli.sh" --file="$cli"
    wait_for_port_release "$port"
    wait_for_port_release "$management_port"
  fi
  echo "Ratchet Showcase: $(showcase_url "$port")"
  mkdir -p "$home/standalone/deployments"
  rm -f "$home"/standalone/deployments/ratchet-showcase.war*
  "$home/bin/standalone.sh" \
    -Djboss.http.port="$port" \
    -Djboss.https.port="$https_port" \
    -Djboss.management.http.port="$management_port" \
    -Djboss.bind.address=127.0.0.1 \
    -Djboss.bind.address.management=127.0.0.1 \
    -Dshowcase.server="$server" \
    -Dshowcase.db="$db" \
    -Dshowcase.version="$SHOWCASE_VERSION" &
  local server_pid=$!
  wildfly_cleanup_home="$home"
  wildfly_cleanup_management_port="$management_port"
  wildfly_cleanup_server_pid="$server_pid"
  trap cleanup_wildfly EXIT
  trap cleanup_wildfly_int INT
  trap cleanup_wildfly_term TERM

  wait_for_wildfly_management "$home" "$management_port" "$server_pid"
  "$home/bin/jboss-cli.sh" \
    --connect \
    --controller=127.0.0.1:"$management_port" \
    --command="deploy $war --force --name=ratchet-showcase.war --runtime-name=$runtime_name"
  wait_for_showcase_http "$port" "$context"
  if [[ "${SHOWCASE_SMOKE:-false}" == "true" ]]; then
    run_showcase_smoke "$port"
    return 0
  fi
  local status=0
  wait "$server_pid" || status=$?
  # Drop the WildFly-specific handlers but keep an EXIT trap so an embedded
  # database container is still removed when the script finishes. The nulled
  # cleanup vars below make cleanup_wildfly a no-op for the (now stopped) server.
  trap cleanup_wildfly EXIT
  trap - INT TERM
  wildfly_cleanup_home=""
  wildfly_cleanup_management_port=""
  wildfly_cleanup_server_pid=""
  return "$status"
}

patch_domain_ports() {
  local domain_xml="$1"
  local http_port="$2"
  local admin_port="$3"
  perl -0pi -e "s/(<network-listener[^>]* port=\")[^\"]+(\"[^>]*name=\"http-listener-1\")/\${1}$http_port\${2}/g" "$domain_xml"
  perl -0pi -e "s/(<network-listener[^>]* port=\")[^\"]+(\"[^>]*name=\"admin-listener\")/\${1}$admin_port\${2}/g" "$domain_xml"
}

asadmin_sql_args() {
  case "$db" in
    postgresql)
      echo "--datasourceclassname org.postgresql.ds.PGSimpleDataSource --restype javax.sql.DataSource --property serverName=$postgres_host:portNumber=$postgres_port:databaseName=$postgres_db:user=$postgres_user:password=$postgres_password"
      ;;
    mysql)
      echo "--datasourceclassname com.mysql.cj.jdbc.MysqlDataSource --restype javax.sql.DataSource --property serverName=$mysql_host:portNumber=$mysql_port:databaseName=$mysql_db:user=$mysql_user:password=$mysql_password"
      ;;
    oracle)
      # VERIFY: asadmin --property is ':'-delimited, so a full Oracle thin URL (which is
      # full of colons) cannot be passed as a single url= property. OracleDataSource builds
      # the URL from discrete properties instead; confirm driverType/serviceName map to your
      # PDB (FREEPDB1) on GlassFish/Payara.
      echo "--datasourceclassname oracle.jdbc.pool.OracleDataSource --restype javax.sql.DataSource --property serverName=$oracle_host:portNumber=$oracle_port:databaseName=$oracle_service:driverType=thin:user=$oracle_user:password=$oracle_password"
      ;;
    sqlserver)
      echo "--datasourceclassname com.microsoft.sqlserver.jdbc.SQLServerDataSource --restype javax.sql.DataSource --property serverName=$sqlserver_host:portNumber=$sqlserver_port:databaseName=$sqlserver_db:user=$sqlserver_user:password=$sqlserver_password:encrypt=true:trustServerCertificate=true"
      ;;
  esac
}

run_glassfish_family() {
  local home="$1"
  local http_port="$2"
  local admin_port="$3"
  select_server_java
  chmod +x "$home/bin/asadmin"
  patch_domain_ports "$home/glassfish/domains/domain1/config/domain.xml" "$http_port" "$admin_port"
  if [[ "$db" != "mongodb" ]]; then
    ensure_sql_driver_available
    run_sql_schema_migration
    mkdir -p "$home/glassfish/domains/domain1/lib"
    cp "$jdbc_dir"/*.jar "$home/glassfish/domains/domain1/lib/"
  fi
  "$home/bin/asadmin" --port "$admin_port" start-domain domain1
  trap '"$home/bin/asadmin" --port "$admin_port" stop-domain domain1 >/dev/null 2>&1 || true; cleanup_db_container' EXIT
  "$home/bin/asadmin" --port "$admin_port" undeploy ratchet-showcase >/dev/null 2>&1 || true
  if [[ "$db" != "mongodb" ]]; then
    "$home/bin/asadmin" --port "$admin_port" delete-jdbc-resource "$ds_jndi" >/dev/null 2>&1 || true
    "$home/bin/asadmin" --port "$admin_port" delete-jdbc-connection-pool RatchetPool >/dev/null 2>&1 || true
    # shellcheck disable=SC2046
    "$home/bin/asadmin" --port "$admin_port" create-jdbc-connection-pool $(asadmin_sql_args) RatchetPool
    "$home/bin/asadmin" --port "$admin_port" create-jdbc-resource --connectionpoolid RatchetPool "$ds_jndi"
  fi
  "$home/bin/asadmin" --port "$admin_port" deploy --force=true --contextroot "$(server_context_root)" "$war"
  echo "Ratchet Showcase: $(showcase_url "$http_port")"
  tail -f "$home/glassfish/domains/domain1/logs/server.log"
}

run_openliberty() {
  local home="${OPENLIBERTY_HOME:?OPENLIBERTY_HOME is required for openliberty-managed}"
  local server_name="${OPENLIBERTY_SERVER_NAME:-defaultServer}"
  local config="${OPENLIBERTY_SERVER_CONFIG_DIR:-$home/usr/servers/$server_name}"
  local http_port="${OPENLIBERTY_HTTP_PORT:-29080}"
  local https_port="${OPENLIBERTY_HTTPS_PORT:-29443}"
  select_server_java
  chmod +x "$home/bin/server"
  mkdir -p "$config/apps" "$config/jdbc"
  cp "$war" "$config/apps/ratchet-showcase.war"
  if [[ "$db" != "mongodb" ]]; then
    ensure_sql_driver_available
    run_sql_schema_migration
    cp "$jdbc_dir"/*.jar "$config/jdbc/"
  fi
  {
    cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<server description="Ratchet Showcase">
  <featureManager>
    <feature>webProfile-10.0</feature>
    <feature>jdbc-4.3</feature>
  </featureManager>
  <httpEndpoint id="defaultHttpEndpoint" host="localhost" httpPort="$http_port" httpsPort="$https_port"/>
  <webApplication location="ratchet-showcase.war" contextRoot="$(server_context_root)"/>
EOF
    if [[ "$db" == "postgresql" ]]; then
      cat <<EOF
  <library id="jdbcLib"><fileset dir="\${server.config.dir}/jdbc" includes="*.jar"/></library>
  <dataSource id="RatchetDS" jndiName="$ds_jndi">
    <jdbcDriver libraryRef="jdbcLib"/>
    <properties.postgresql serverName="$postgres_host" portNumber="$postgres_port" databaseName="$postgres_db" user="$postgres_user" password="$postgres_password"/>
  </dataSource>
EOF
    elif [[ "$db" == "mysql" ]]; then
      cat <<EOF
  <library id="jdbcLib"><fileset dir="\${server.config.dir}/jdbc" includes="*.jar"/></library>
  <dataSource id="RatchetDS" jndiName="$ds_jndi">
    <jdbcDriver libraryRef="jdbcLib"/>
    <properties.mysql serverName="$mysql_host" portNumber="$mysql_port" databaseName="$mysql_db" user="$mysql_user" password="$mysql_password"/>
  </dataSource>
EOF
    elif [[ "$db" == "oracle" ]]; then
      cat <<EOF
  <library id="jdbcLib"><fileset dir="\${server.config.dir}/jdbc" includes="*.jar"/></library>
  <dataSource id="RatchetDS" jndiName="$ds_jndi">
    <jdbcDriver libraryRef="jdbcLib"/>
    <properties.oracle URL="$(jdbc_url)" user="$oracle_user" password="$oracle_password"/>
  </dataSource>
EOF
    elif [[ "$db" == "sqlserver" ]]; then
      # Open Liberty defaults SQL Server data sources to TRANSACTION_REPEATABLE_READ; Ratchet's
      # startup isolation check requires READ COMMITTED, so pin it explicitly.
      cat <<EOF
  <library id="jdbcLib"><fileset dir="\${server.config.dir}/jdbc" includes="*.jar"/></library>
  <dataSource id="RatchetDS" jndiName="$ds_jndi" isolationLevel="TRANSACTION_READ_COMMITTED">
    <jdbcDriver libraryRef="jdbcLib"/>
    <properties.microsoft.sqlserver serverName="$sqlserver_host" portNumber="$sqlserver_port" databaseName="$sqlserver_db" user="$sqlserver_user" password="$sqlserver_password" encrypt="true" trustServerCertificate="true"/>
  </dataSource>
EOF
    fi
    cat <<EOF
</server>
EOF
  } > "$config/server.xml"
  echo "Ratchet Showcase: http://localhost:$http_port$context"
  # Run in the foreground (not exec) so the script's EXIT/INT/TERM traps still
  # fire and remove an embedded database container when the server stops.
  "$home/bin/server" run "$server_name"
}

maybe_start_embedded_db

case "$server" in
  wildfly)
    run_wildfly
    ;;
  payara)
    run_glassfish_family "${PAYARA_HOME:?PAYARA_HOME is required for payara-managed}" "${PAYARA_HTTP_PORT:-18080}" "${PAYARA_ADMIN_PORT:-14848}"
    ;;
  glassfish)
    run_glassfish_family "${GLASSFISH_HOME:?GLASSFISH_HOME is required for glassfish-managed}" "${GLASSFISH_HTTP_PORT:-19080}" "${GLASSFISH_ADMIN_PORT:-15848}"
    ;;
  openliberty)
    run_openliberty
    ;;
  *)
    echo "Activate one server profile: wildfly-managed, payara-managed, openliberty-managed, or glassfish-managed." >&2
    exit 2
    ;;
esac
