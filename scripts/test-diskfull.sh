#!/bin/sh
# Disk-full rollback test (tests/test_disk_full.py) on both engines, in Docker:
# a 1 MiB tmpfs gives a real ENOSPC without privileges. The repo is copied into
# the container, so no Linux build output lands in the working tree.
set -eu
cd "$(dirname "$0")/.."
docker run --rm \
  --tmpfs /small:size=1m \
  -v "$PWD:/src:ro" \
  -v durastream-cargo:/usr/local/cargo/registry \
  -v durastream-uv:/root/.cache/uv \
  -e DURASTREAM_SMALL_FS=/small \
  rust:1-bookworm sh -euc '
    curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1
    export PATH="$HOME/.local/bin:$PATH"
    mkdir /work
    tar -C /src --exclude=./.venv --exclude=./core/target --exclude=./dist \
      --exclude="*.so" --exclude=__pycache__ -cf - . | tar -xf - -C /work
    cd /work
    uv sync -q
    uv run python -c "import durastream as d; assert d.ENGINE == \"native\", d.ENGINE"
    uv run pytest -q tests/test_disk_full.py
    DURASTREAM_PURE=1 uv run pytest -q tests/test_disk_full.py
  '
