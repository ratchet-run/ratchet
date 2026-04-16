ARG STORE=postgresql
ARG EXTRA_PROFILES=
ARG WILDFLY_SERVER_CONFIG=standalone.xml

FROM maven:3.9.9-eclipse-temurin-17 AS build
ARG STORE=postgresql
ARG EXTRA_PROFILES=
WORKDIR /workspace
COPY . .
RUN mvn -pl ratchet-loadtest -am -P${STORE}${EXTRA_PROFILES:+,${EXTRA_PROFILES}} -DskipTests package

FROM quay.io/wildfly/wildfly:39.0.1.Final-2-jdk17
ARG STORE=postgresql
ARG WILDFLY_SERVER_CONFIG=standalone.xml

ENV WILDFLY_SERVER_CONFIG=${WILDFLY_SERVER_CONFIG}

USER root
RUN mkdir -p \
    /opt/jboss/ratchet/drivers \
    /opt/jboss/wildfly/standalone/data/content \
    /opt/jboss/wildfly/standalone/log \
    /opt/jboss/wildfly/standalone/tmp
COPY --from=build /root/.m2/repository/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar /opt/jboss/ratchet/drivers/postgresql.jar
COPY --from=build /root/.m2/repository/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar /opt/jboss/ratchet/drivers/mysql.jar
COPY infra/loadtest/wildfly/${STORE}.cli /opt/jboss/ratchet/store.cli
COPY --from=build /workspace/ratchet-loadtest/target/ratchet-loadtest-0.1.0-SNAPSHOT.war /opt/jboss/wildfly/standalone/deployments/ROOT.war
RUN chown -R jboss:root /opt/jboss/ratchet /opt/jboss/wildfly/standalone \
    && chmod -R ug+rwX /opt/jboss/ratchet /opt/jboss/wildfly/standalone \
    && /opt/jboss/wildfly/bin/jboss-cli.sh --file=/opt/jboss/ratchet/store.cli \
    && rm -rf /opt/jboss/wildfly/standalone/configuration/standalone_xml_history/* \
    && chown -R jboss:root /opt/jboss/ratchet /opt/jboss/wildfly/standalone \
    && chmod -R ug+rwX /opt/jboss/ratchet /opt/jboss/wildfly/standalone

USER jboss
EXPOSE 8080 9990
CMD ["/bin/sh", "-c", "exec /opt/jboss/wildfly/bin/standalone.sh -c ${WILDFLY_SERVER_CONFIG:-standalone.xml} -b 0.0.0.0 -Djboss.node.name=${NODE_ID:-${HOSTNAME}}"]
