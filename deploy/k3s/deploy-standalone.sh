#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
MASTER=${MASTER:-k3s-master}
PVE=${PVE:-root@192.168.31.171}
PVE_KEY=${PVE_KEY:-$HOME/.ssh/id_ed25519_pve}
# Mandatory resource gate; never start Flink while the build VM is running.
ssh -o BatchMode=yes -i "$PVE_KEY" "$PVE" 'test "$(qm status 107)" = "status: stopped"'
: "${JAVA_HOME:?Set JAVA_HOME to your Java 17 installation}"
mvn -pl realtime_dw -am -DskipTests compile
mkdir -p .artifacts/standalone
"$JAVA_HOME/bin/jar" cf .artifacts/standalone/standalone-events.jar \
  -C realtime_dw/target/classes com/test/bigdata/rtdw/deployment
JOB_SHA=$(shasum -a 256 .artifacts/standalone/standalone-events.jar | cut -c1-12)
JOB_CONFIGMAP="standalone-events-job-$JOB_SHA"
sed "s/standalone-events-job/$JOB_CONFIGMAP/g" deploy/k3s/flink-standalone.yaml \
  > .artifacts/standalone/flink-standalone.yaml
scp .artifacts/standalone/standalone-events.jar .artifacts/standalone/flink-standalone.yaml \
  deploy/k3s/flink-ui-nodeport.yaml deploy/k3s/namespace.yaml \
  deploy/k3s/flink-rbac.yaml deploy/k3s/flink-state-pvc.yaml "$MASTER:/var/tmp/"
ssh -o BatchMode=yes "$MASTER" \
  'kubectl apply -f /var/tmp/namespace.yaml && kubectl apply -f /var/tmp/flink-rbac.yaml && kubectl apply -f /var/tmp/flink-state-pvc.yaml'
ssh -o BatchMode=yes "$MASTER" \
  "kubectl -n doit create configmap $JOB_CONFIGMAP --from-file=standalone-events.jar=/var/tmp/standalone-events.jar --dry-run=client -o yaml > /var/tmp/standalone-job-configmap.yaml && kubectl apply -f /var/tmp/standalone-job-configmap.yaml"
# Recheck immediately before creating or resuming the Flink workload.
ssh -o BatchMode=yes -i "$PVE_KEY" "$PVE" 'test "$(qm status 107)" = "status: stopped"'
ssh -o BatchMode=yes "$MASTER" \
  'kubectl apply -f /var/tmp/flink-standalone.yaml && kubectl apply -f /var/tmp/flink-ui-nodeport.yaml'
echo 'Check http://192.168.31.88:30816 and /jobs/overview; wait for RUNNING and completed checkpoints.'
