#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

IMAGE_NAMESPACE="${IMAGE_NAMESPACE:-doit36}"
# Native Linux Podman otherwise tags an unqualified image under localhost/,
# while K3s resolves the deployment's doit36/... reference under docker.io/.
case "${IMAGE_NAMESPACE%%/*}" in
  *.*|*:*|localhost) ;;
  *) IMAGE_NAMESPACE="docker.io/${IMAGE_NAMESPACE}" ;;
esac
IMAGE_TAG="${IMAGE_TAG:-local}"
IMAGE_NAME="${IMAGE_NAME:-flink-jobs}"
IMAGE_REF="${IMAGE_NAMESPACE}/${IMAGE_NAME}:${IMAGE_TAG}"
RUNTIME_JDK="${RUNTIME_JDK:-17}"
TEMURIN_IMAGE="${TEMURIN_IMAGE:-docker.io/library/eclipse-temurin:${RUNTIME_JDK}-jre-jammy}"
FLINK_IMAGE="${FLINK_IMAGE:-docker.io/apache/flink:1.19.3-scala_2.12-java17}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/deploy/k3s/output}"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/deploy/k3s/.image-build}"
SAVE_TAR="${SAVE_TAR:-true}"
PODMAN_BIN="${PODMAN_BIN:-podman}"
IMAGE_PLATFORM="${IMAGE_PLATFORM:-linux/amd64}"

modules=(realtime_dw rt_rule_mk)

case "${RUNTIME_JDK}" in
  17|21|25) ;;
  *)
    echo "unsupported RUNTIME_JDK=${RUNTIME_JDK}; supported LTS versions: 17, 21, 25" >&2
    exit 2
    ;;
esac

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}/usrlib" "${OUTPUT_DIR}"

for module in "${modules[@]}"; do
  classes_dir="${ROOT_DIR}/${module}/target/classes"
  job_jar="${BUILD_DIR}/usrlib/${module}/${module}-job.jar"
  dependency_dir="${ROOT_DIR}/${module}/target/k3s-libs"

  if [[ ! -d "${classes_dir}" ]]; then
    echo "missing compiled classes: ${classes_dir}" >&2
    echo "run 'mvn -DskipTests clean compile' before building the image" >&2
    exit 1
  fi

  mkdir -p "${BUILD_DIR}/usrlib/${module}"
  # This creates a Flink job JAR from compiled classes; it deliberately does
  # not invoke Maven's package phase.
  jar --create --file "${job_jar}" -C "${classes_dir}" .

  rm -rf "${dependency_dir}"
  mvn --batch-mode --no-transfer-progress -q -pl "${module}" \
    dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory="target/k3s-libs"
  cp -R "${dependency_dir}/." "${BUILD_DIR}/usrlib/${module}/"
done

echo "building ${IMAGE_REF}"
"${PODMAN_BIN}" build \
  --platform "${IMAGE_PLATFORM}" \
  --build-arg "RUNTIME_JDK=${RUNTIME_JDK}" \
  --build-arg "TEMURIN_IMAGE=${TEMURIN_IMAGE}" \
  --build-arg "FLINK_IMAGE=${FLINK_IMAGE}" \
  --file "${ROOT_DIR}/deploy/k3s/Dockerfile.flink-job" \
  --tag "${IMAGE_REF}" \
  "${BUILD_DIR}"

if [[ "${SAVE_TAR}" == "true" ]]; then
  tar_path="${OUTPUT_DIR}/${IMAGE_NAME}-${IMAGE_TAG}.tar"
  echo "saving ${IMAGE_REF} to ${tar_path}"
  "${PODMAN_BIN}" save --format docker-archive --output "${tar_path}" "${IMAGE_REF}"
fi

cat <<EOF
image ready: ${IMAGE_REF}

Import the exported tar on every K3s node that can run this job:
  sudo k3s ctr images import ${OUTPUT_DIR}/${IMAGE_NAME}-${IMAGE_TAG}.tar
EOF
