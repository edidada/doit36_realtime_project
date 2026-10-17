#!/usr/bin/env bash
# Prepare fixed-node acceptance services; never modifies other namespaces.
set -euo pipefail
cd "$(dirname "$0")/../../.."
ssh -o BatchMode=yes root@192.168.31.211 'bash -se' <<'REMOTE'
set -euo pipefail
mkdir -p /var/lib/doit36/downloads
cd /var/lib/doit36/downloads
if ! k3s crictl inspecti docker.m.daocloud.io/apache/kafka:3.9.1 >/dev/null 2>&1; then
  k3s ctr images pull --platform linux/amd64 docker.m.daocloud.io/apache/kafka:3.9.1
fi
java_image=docker.m.daocloud.io/library/eclipse-temurin@sha256:8b97d189ef6fdd9d99fc455d1542647f9ac5eeffc30681ec61d2f29e7acabf16
if ! k3s crictl inspecti docker.m.daocloud.io/library/eclipse-temurin:11-jdk-jammy >/dev/null 2>&1; then
  k3s ctr images pull --platform linux/amd64 "$java_image"
  k3s ctr images tag --force "$java_image" docker.m.daocloud.io/library/eclipse-temurin:11-jdk-jammy
fi
if [ ! -x /var/lib/doit36/hbase-2.4.18/bin/hbase ]; then
  curl -fL --retry 3 -o hbase-2.4.18-bin.tar.gz.part https://repo.huaweicloud.com/apache/hbase/2.4.18/hbase-2.4.18-bin.tar.gz
  curl -fL --retry 3 -o hbase.sha512 https://archive.apache.org/dist/hbase/2.4.18/hbase-2.4.18-bin.tar.gz.sha512
  expected=$(cut -d: -f2 hbase.sha512 | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')
  actual=$(sha512sum hbase-2.4.18-bin.tar.gz.part | cut -d' ' -f1)
  test "$expected" = "$actual"
  mv hbase-2.4.18-bin.tar.gz.part hbase-2.4.18-bin.tar.gz
  tar -xzf hbase-2.4.18-bin.tar.gz -C /var/lib/doit36
fi
REMOTE
ssh -o BatchMode=yes root@192.168.31.251 'bash -se' <<'REMOTE'
set -euo pipefail
mkdir -p /var/lib/doit36/downloads
cd /var/lib/doit36/downloads
if ! k3s crictl inspecti docker.m.daocloud.io/apache/doris:fe-2.1.8 >/dev/null 2>&1; then
  k3s ctr images pull --platform linux/amd64 docker.m.daocloud.io/apache/doris:fe-2.1.8
fi
if [ ! -x /var/lib/doit36/apache-doris-2.1.8-bin-x64-noavx2/be/lib/doris_be ]; then
  archive=apache-doris-2.1.8-bin-x64-noavx2.tar.gz
  curl -fL --retry 3 -o "$archive.part" "https://apache-doris-releases.oss-accelerate.aliyuncs.com/$archive"
  curl -fL --retry 3 -o doris-noavx.sha512 "https://apache-doris-releases.oss-accelerate.aliyuncs.com/$archive.sha512"
  expected=$(awk '{print $1}' doris-noavx.sha512)
  actual=$(sha512sum "$archive.part" | cut -d' ' -f1)
  test "$expected" = "$actual"
  mv "$archive.part" "$archive"
  tar -xzf "$archive" -C /var/lib/doit36 --wildcards '*/be/*'
fi
if [ ! -f vm-max-map-count.before ]; then sysctl -n vm.max_map_count > vm-max-map-count.before; fi
printf 'vm.max_map_count = 2000000\n' > /etc/sysctl.d/90-doit36-doris.conf
sysctl -p /etc/sysctl.d/90-doit36-doris.conf
REMOTE
scp deploy/k3s/namespace.yaml deploy/k3s/business/middleware.yaml k3s-master:/var/tmp/
ssh -o BatchMode=yes k3s-master 'kubectl apply -f /var/tmp/namespace.yaml && kubectl apply -f /var/tmp/middleware.yaml'
