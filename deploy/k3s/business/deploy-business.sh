#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
: "${JAVA_HOME:?Set JAVA_HOME to Java 17}"
MASTER=${MASTER:-k3s-master}
PVE_KEY=${PVE_KEY:-$HOME/.ssh/id_ed25519_pve}
ssh -o BatchMode=yes -i "$PVE_KEY" root@192.168.31.171 'test "$(qm status 107)" = "status: stopped"'
mvn -pl realtime_dw -am -DskipTests compile
mkdir -p .artifacts/business
"$JAVA_HOME/bin/jar" cf .artifacts/business/business-etl.jar \
  -C realtime_dw/target/classes cn/doitedu/rtdw/data_etl/EtlJob01_UserEventsLogCommonDim.class \
  -C realtime_dw/target/classes 'cn/doitedu/rtdw/data_etl/EtlJob01_UserEventsLogCommonDim$HashMap2Json.class' \
  -C realtime_dw/target/classes 'cn/doitedu/rtdw/data_etl/EtlJob01_UserEventsLogCommonDim$GeoHashFunction.class'
JOB_SHA=$(shasum -a 256 .artifacts/business/business-etl.jar | cut -c1-12)
JOB_CONFIGMAP=business-etl-job-$JOB_SHA
sed "s/business-etl-job/$JOB_CONFIGMAP/g" deploy/k3s/business/flink-business.yaml > .artifacts/business/flink-business.yaml
scp .artifacts/business/business-etl.jar .artifacts/business/flink-business.yaml \
  deploy/k3s/business/init-hbase.rb deploy/k3s/business/init-doris.sql \
  deploy/k3s/business/init-on-master.py deploy/k3s/business/verify-business.py \
  deploy/k3s/flink-rbac.yaml deploy/k3s/flink-state-pvc.yaml deploy/k3s/flink-ui-nodeport.yaml "$MASTER:/var/tmp/"
ssh -o BatchMode=yes "$MASTER" 'cd /var/tmp && python3 init-on-master.py'
ssh -o BatchMode=yes "$MASTER" \
  "kubectl -n doit create configmap $JOB_CONFIGMAP --from-file=business-etl.jar=/var/tmp/business-etl.jar --dry-run=client -o yaml > /var/tmp/business-job-cm.yaml && kubectl apply -f /var/tmp/business-job-cm.yaml"
ssh -o BatchMode=yes "$MASTER" 'kubectl apply -f /var/tmp/flink-rbac.yaml && kubectl apply -f /var/tmp/flink-state-pvc.yaml'
ssh -o BatchMode=yes -i "$PVE_KEY" root@192.168.31.171 'test "$(qm status 107)" = "status: stopped"'
ssh -o BatchMode=yes "$MASTER" 'kubectl apply -f /var/tmp/flink-business.yaml && kubectl apply -f /var/tmp/flink-ui-nodeport.yaml'
# Reconciliation can take minutes; the verifier waits for the business tasks to run.
ssh -o BatchMode=yes "$MASTER" 'cd /var/tmp && python3 verify-business.py'
