#!/usr/bin/env bash
set -euo pipefail
echo 'This legacy stateless hot-patch deployment has been retired.' >&2
echo 'It targeted the deleted doit namespace and a Flink 1.16 image.' >&2
echo 'Use scripts/prepare-k3s-runtime.ps1 and scripts/k3s-stateful-upgrade.py instead.' >&2
echo 'Read docs/k3s-redeploy-2026-11-01.md; verify VM107 stopped and back up savepoints first.' >&2
exit 2
