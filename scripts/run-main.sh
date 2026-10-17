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
# Prefer the local validated profile when no explicit profile was selected.
if [[ -z "${MAIN_ENV_JSON:-}" && "${MAIN_ENV_LOADED:-false}" != true && -f "$ROOT_DIR/.artifacts/main-audit/k3s/runtime-env.json" ]]; then
  export MAIN_ENV_JSON="$ROOT_DIR/.artifacts/main-audit/k3s/runtime-env.json"
fi
# Optional private JSON profile, loaded without shell evaluation or credential output.
if [[ -n "${MAIN_ENV_JSON:-}" ]]; then
  exec python3 - "$ROOT_DIR/scripts/run-main.sh" "$module" "$main" "$@" <<'PYENV'
import json, os, sys
with open(os.environ['MAIN_ENV_JSON']) as profile:
    settings = json.load(profile)
if not isinstance(settings, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in settings.items()):
    raise SystemExit('MAIN_ENV_JSON must contain a JSON object of string environment values')
os.environ.update(settings)
os.environ.pop('MAIN_ENV_JSON', None)
os.environ['MAIN_ENV_LOADED'] = 'true'
os.execvp('bash', ['bash'] + sys.argv[1:])
PYENV
fi
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
mkdir -p "$ROOT_DIR/logs/runtime"
classpath="$ROOT_DIR/$module/target/classes:$ROOT_DIR/$module/target/test-classes:$(cat "$module/target/main-audit-classpath.txt")"
exec "$java_bin" "@$ROOT_DIR/scripts/java17.options" -Xmx768m -XX:ActiveProcessorCount=2 \
  -Dmain.class="$main" -Dmain.log.dir="$ROOT_DIR/logs/runtime" \
  -cp "$classpath" "$main" "$@"
