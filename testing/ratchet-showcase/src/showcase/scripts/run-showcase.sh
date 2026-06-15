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

export MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
export MONGO_DATABASE="${MONGO_DATABASE:-ratchet}"

jdbc_dir="$build_dir/showcase-jdbc"
ds_jndi="${SHOWCASE_DATASOURCE_JNDI:-java:jboss/datasources/RatchetDS}"

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

ensure_sql_driver_available() {
  if [[ "$db" == "postgresql" && -z "$(postgres_jar)" ]]; then
    echo "PostgreSQL driver missing under $jdbc_dir. Run Maven package first." >&2
    exit 1
  fi
  if [[ "$db" == "mysql" && -z "$(mysql_jar)" ]]; then
    echo "MySQL driver missing under $jdbc_dir. Run Maven package first." >&2
    exit 1
  fi
}

jdbc_url() {
  if [[ "$db" == "postgresql" ]]; then
    echo "jdbc:postgresql://$postgres_host:$postgres_port/$postgres_db"
  else
    echo "jdbc:mysql://$mysql_host:$mysql_port/$mysql_db"
  fi
}

jdbc_user() {
  if [[ "$db" == "postgresql" ]]; then
    echo "$postgres_user"
  else
    echo "$mysql_user"
  fi
}

jdbc_password() {
  if [[ "$db" == "postgresql" ]]; then
    echo "$postgres_password"
  else
    echo "$mysql_password"
  fi
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
  mkdir -p "$home/modules/org/postgresql/main" "$home/modules/com/mysql/main"
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
    if [[ "$db" == "postgresql" ]]; then
      cat > "$cli" <<EOF
embed-server --server-config=standalone.xml --std-out=echo
if (outcome != success) of /subsystem=datasources/jdbc-driver=postgresql:read-resource
  /subsystem=datasources/jdbc-driver=postgresql:add(driver-name=postgresql,driver-module-name=org.postgresql,driver-class-name=org.postgresql.Driver)
end-if
if (outcome != success) of /subsystem=datasources/data-source=RatchetDS:read-resource
  data-source add --name=RatchetDS --jndi-name=$ds_jndi --driver-name=postgresql --connection-url=jdbc:postgresql://$postgres_host:$postgres_port/$postgres_db --user-name=$postgres_user --password=$postgres_password --enabled=true
end-if
stop-embedded-server
EOF
    else
      cat > "$cli" <<EOF
embed-server --server-config=standalone.xml --std-out=echo
if (outcome != success) of /subsystem=datasources/jdbc-driver=mysql:read-resource
  /subsystem=datasources/jdbc-driver=mysql:add(driver-name=mysql,driver-module-name=com.mysql,driver-class-name=com.mysql.cj.jdbc.Driver)
end-if
if (outcome != success) of /subsystem=datasources/data-source=RatchetDS:read-resource
  data-source add --name=RatchetDS --jndi-name=$ds_jndi --driver-name=mysql --connection-url=jdbc:mysql://$mysql_host:$mysql_port/$mysql_db --user-name=$mysql_user --password=$mysql_password --enabled=true
end-if
stop-embedded-server
EOF
    fi
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
  local status=0
  wait "$server_pid" || status=$?
  trap - EXIT INT TERM
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
  if [[ "$db" == "postgresql" ]]; then
    echo "--datasourceclassname org.postgresql.ds.PGSimpleDataSource --restype javax.sql.DataSource --property serverName=$postgres_host:portNumber=$postgres_port:databaseName=$postgres_db:user=$postgres_user:password=$postgres_password"
  else
    echo "--datasourceclassname com.mysql.cj.jdbc.MysqlDataSource --restype javax.sql.DataSource --property serverName=$mysql_host:portNumber=$mysql_port:databaseName=$mysql_db:user=$mysql_user:password=$mysql_password"
  fi
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
  trap '"$home/bin/asadmin" --port "$admin_port" stop-domain domain1 >/dev/null 2>&1 || true' EXIT
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
    fi
    cat <<EOF
</server>
EOF
  } > "$config/server.xml"
  echo "Ratchet Showcase: http://localhost:$http_port$context"
  exec "$home/bin/server" run "$server_name"
}

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
