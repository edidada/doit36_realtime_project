#!/usr/bin/env bash
# Run a main in a separate Java 17 JVM, with Flink's distribution JVM options.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <module> <fully.qualified.Main> [arguments...]" >&2
  exit 2
fi
module="$1"
main="$2"
shift 2
case "$module" in realtime_dw|rt_rule_mk|redis_demo|batch_jobs) ;; *) echo 'Unknown module' >&2; exit 2 ;; esac
cd "$ROOT_DIR"
java_bin="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
if [[ "$("$java_bin" -XshowSettings:properties -version 2>&1 | sed -n 's/.*java.specification.version = //p')" != 17 ]]; then
  echo 'Set JAVA_HOME to a JDK 17 installation.' >&2
  exit 2
fi
if [[ "${SKIP_MAIN_COMPILE:-false}" != true ]]; then
  mvn -B -ntp -pl "$module" -am test-compile
  mvn -B -ntp -q -pl "$module" dependency:build-classpath \
    -Dmdep.includeScope=test -Dmdep.outputFile=target/main-audit-classpath.txt
fi
classpath="$ROOT_DIR/$module/target/classes:$ROOT_DIR/$module/target/test-classes:$(cat "$module/target/main-audit-classpath.txt")"
exec "$java_bin" "@$ROOT_DIR/scripts/java17.options" -Xmx768m -XX:ActiveProcessorCount=2 \
  -cp "$classpath" "$main" "$@"
