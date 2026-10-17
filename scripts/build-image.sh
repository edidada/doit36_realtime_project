#!/usr/bin/env bash
# Compile the Java modules, build with our source-built Podman and export tar.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_ROOT="${PODMAN_BUILD_ROOT:-${ROOT_DIR}/.artifacts/podman-build}"
PODMAN_BIN="${BUILD_ROOT}/bin/podman"
if [[ ! -x "${PODMAN_BIN}" ]]; then
  echo 'Run bash scripts/build-podman.sh first.' >&2
  exit 1
fi
PODMAN_BIN="$(cd "$(dirname "${PODMAN_BIN}")" && pwd)/podman"
[[ "$("${PODMAN_BIN}" --version)" == 'podman version 4.9.5' ]] || {
  echo 'Expected the source-built Podman 4.9.5.' >&2; exit 1;
}
case "$(uname -s)" in
  Darwin)
    export CONTAINERS_MACHINE_PROVIDER=qemu
    if [[ "${INIT_PODMAN_MACHINE:-false}" == true ]]; then
      machine="${PODMAN_MACHINE_NAME:-doit36-build}"
      case "$(uname -m)" in
        x86_64) qemu=qemu-system-x86_64 ;;
        arm64) qemu=qemu-system-aarch64 ;;
      esac
      command -v "${qemu}" >/dev/null || { echo 'Install QEMU first (see scripts/README.md).' >&2; exit 1; }
      if ! "${PODMAN_BIN}" machine inspect "${machine}" >/dev/null 2>&1; then
        init_args=(--cpus 2 --memory 4096 --disk-size 30 --volume "${ROOT_DIR}:${ROOT_DIR}")
        if [[ -n "${PODMAN_MACHINE_IMAGE:-}" ]]; then
          init_args+=(--image-path "${PODMAN_MACHINE_IMAGE}")
        fi
        "${PODMAN_BIN}" machine init "${init_args[@]}" "${machine}"
      fi
      if [[ "$("${PODMAN_BIN}" machine inspect --format '{{.State}}' "${machine}")" != running ]]; then
        "${PODMAN_BIN}" machine start "${machine}"
      fi
      # Select this connection for this process, without changing user defaults.
      export CONTAINER_CONNECTION="${machine}"
    fi
    ;;
  Linux) ;;
  *) echo 'Only macOS and Linux are supported (Windows is excluded).' >&2; exit 2 ;;
esac
"${PODMAN_BIN}" info >/dev/null
cd "${ROOT_DIR}"
if [[ "${SKIP_MAVEN_COMPILE:-false}" != true ]]; then
  mvn --batch-mode --no-transfer-progress --show-version -DskipTests clean compile
fi
export PODMAN_BIN
export IMAGE_PLATFORM="${IMAGE_PLATFORM:-linux/amd64}"
export SAVE_TAR=true
bash "${ROOT_DIR}/deploy/k3s/build-k3s-image.sh"
