#!/usr/bin/env bash
# Build the pinned release without replacing the system Podman or Go.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_ROOT="${PODMAN_BUILD_ROOT:-${ROOT_DIR}/.artifacts/podman-build}"
mkdir -p "${BUILD_ROOT}"
BUILD_ROOT="$(cd "${BUILD_ROOT}" && pwd)"
PODMAN_VERSION=4.9.5
GO_VERSION=1.24.6
export GOTOOLCHAIN=local GOWORK=off GO111MODULE=on
export GOPATH="${BUILD_ROOT}/go"
export GOPROXY="${GOPROXY:-https://proxy.golang.org,direct}"
export GOMAXPROCS="${GOMAXPROCS:-4}"

case "$(uname -s)" in
  Darwin) platform=darwin ;;
  Linux) platform=linux ;;
  *) echo 'Only macOS and Linux are supported (Windows is excluded).' >&2; exit 2 ;;
esac
case "$(uname -m)" in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) echo 'Only amd64 and arm64 are supported.' >&2; exit 2 ;;
esac
for tool in curl python3 make tar; do
  command -v "${tool}" >/dev/null || { echo "Missing dependency: ${tool}" >&2; exit 1; }
done

# Go 1.24.6 runs on this project's macOS 12.7 host. Do not select Go latest.
# Checksums: https://go.dev/dl/ (go1.24.6).
case "${platform}/${arch}" in
  darwin/amd64) go_sha=4a8d7a32052f223e71faab424a69430455b27b3fff5f4e651f9d97c3e51a8746 ;;
  darwin/arm64) go_sha=4e29202c49573b953be7cc3500e1f8d9e66ddd12faa8cf0939a4951411e09a2a ;;
  linux/amd64) go_sha=bbca37cc395c974ffa4893ee35819ad23ebb27426df87af92e93a9ec66ef8712 ;;
  linux/arm64) go_sha=124ea6033a8bf98aa9fbab53e58d134905262d45a022af3a90b73320f3c3afd5 ;;
esac
if command -v go >/dev/null && [[ "$(go env GOVERSION)" == "go${GO_VERSION}" ]]; then
  go_bin="$(command -v go)"
else
  toolchain="${BUILD_ROOT}/toolchains/go${GO_VERSION}-${platform}-${arch}"
  if [[ ! -x "${toolchain}/go/bin/go" ]]; then
    archive="${BUILD_ROOT}/go${GO_VERSION}.${platform}-${arch}.tar.gz"
    curl --fail --location --retry 3 --connect-timeout 20 --max-time 1800 \
      "${GO_DOWNLOAD_BASE:-https://go.dev/dl}/go${GO_VERSION}.${platform}-${arch}.tar.gz" \
      --output "${archive}.part"
    python3 - "${archive}.part" "${go_sha}" <<'PY'
import hashlib, sys
with open(sys.argv[1], 'rb') as f:
    digest = hashlib.sha256(f.read()).hexdigest()
if digest != sys.argv[2]:
    raise SystemExit('Go archive SHA256 mismatch')
PY
    mv "${archive}.part" "${archive}"
    mkdir -p "${toolchain}"
    tar -xzf "${archive}" -C "${toolchain}"
  fi
  export GOROOT="${toolchain}/go"
  export PATH="${GOROOT}/bin:${PATH}"
  go_bin="${GOROOT}/bin/go"
fi
[[ "$("${go_bin}" env GOVERSION)" == "go${GO_VERSION}" ]]
"${go_bin}" version

# Download a Go module release, verify its known checksum, then copy it out of
# the read-only module cache. Never build inside or chmod the shared cache.
mkdir -p "${BUILD_ROOT}/work" "${BUILD_ROOT}/bin"
cd "${BUILD_ROOT}/work"
GOFLAGS= "${go_bin}" mod download -json \
  "github.com/containers/podman/v4@v${PODMAN_VERSION}" > "${BUILD_ROOT}/source-module.json"
source_dir="${BUILD_ROOT}/work/podman-v${PODMAN_VERSION}-${platform}-${arch}"
python3 - "${BUILD_ROOT}/source-module.json" "${source_dir}" <<'PY'
import json, os, shutil, stat, sys
with open(sys.argv[1]) as f:
    module = json.load(f)
if module.get('Sum') != 'h1:ra09gsEaxCxmFG5AEr01D0zQ7k5VGlyaUOQhVUom54E=':
    raise SystemExit('Podman v4.9.5 source checksum mismatch')
if not os.path.isdir(sys.argv[2]):
    shutil.copytree(module['Dir'], sys.argv[2])
    os.chmod(sys.argv[2], os.stat(sys.argv[2]).st_mode | stat.S_IWUSR)
    for root, dirs, files in os.walk(sys.argv[2]):
        for name in dirs + files:
            path = os.path.join(root, name)
            os.chmod(path, os.stat(path).st_mode | stat.S_IWUSR)
PY
cd "${source_dir}"
chmod u+x hack/*.sh
if [[ "${platform}" == darwin ]]; then
  CGO_ENABLED=0 make -B podman-remote GO="${go_bin}" \
    GOFLAGS='-mod=readonly -trimpath -buildvcs=false' GIT_COMMIT= BUILDTAGS= \
    HELPER_BINARIES_DIR="${BUILD_ROOT}/bin"
  cp bin/darwin/podman "${BUILD_ROOT}/bin/podman"
  # QEMU is external; gvproxy is small enough to build at its matching tag.
  GOBIN="${BUILD_ROOT}/bin" CGO_ENABLED=0 GOFLAGS='-mod=readonly -trimpath -buildvcs=false' \
    "${go_bin}" install github.com/containers/gvisor-tap-vsock/cmd/gvproxy@v0.7.2
else
  command -v pkg-config >/dev/null && pkg-config --exists libseccomp || {
    echo 'Install a C compiler, pkg-config and libseccomp development headers; see scripts/README.md.' >&2
    exit 1
  }
  CGO_ENABLED=1 make -B podman GO="${go_bin}" \
    GOFLAGS='-mod=readonly -trimpath -buildvcs=false' GIT_COMMIT= \
    BUILDTAGS='seccomp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper containers_image_openpgp'
  cp bin/podman "${BUILD_ROOT}/bin/podman"
fi
"${BUILD_ROOT}/bin/podman" --version
echo "Compiled Podman: ${BUILD_ROOT}/bin/podman"
