#!/usr/bin/env bash
# Runs on a main-push cache writer after verifying its PostgreSQL consumer.
set -euo pipefail

framework=$1
version=$2
repository=$3
mode=$4
stores=(mysql postgresql mongodb oracle sqlserver)

store_modules=()
for store in "${stores[@]}"; do
  store_modules+=(":ratchet-store-$store")
done
store_selection=$(IFS=,; echo "${store_modules[*]}")
maven=(mvn -B -ntp "-Dmaven.repo.local=$repository")
"${maven[@]}" -pl "$store_selection" -Pgithub -am install \
  -DskipTests -Dspotbugs.skip=true -Dspotless.skip=true

consumer_pom="integrations/ratchet-$framework/consumer-tests/pom.xml"
resolve=("${maven[@]}" -f "$consumer_pom")
profiles=()
if [[ $framework == spring-boot ]]; then
  resolve+=("-Dspring-boot.version=$version")
  if [[ $mode == native ]]; then
    profiles=("-Paot,native")
  fi
else
  profiles=("-P$version")
  if [[ $mode == native ]]; then
    profiles=("-P$version,native")
    resolve+=(-Dgoogle-java-format.version=1.35.0)
  fi
fi

for store in "${stores[@]}"; do
  consumer=:sql-consumer
  if [[ $store == mongodb ]]; then
    consumer=:mongodb-consumer
  fi
  "${resolve[@]}" -pl "$consumer" -am "-Dstore=$store" "${profiles[@]}" dependency:go-offline
done

if [[ $framework == spring-boot && $mode == jvm ]]; then
  "${resolve[@]}" -pl :sql-consumer -am -Dstore=postgresql -Peclipselink,postgresql dependency:go-offline
fi
