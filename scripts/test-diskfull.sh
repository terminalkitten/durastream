#!/bin/sh
# Disk-full rollback test (tests/test_disk_full.py) in Docker: a 1 MiB tmpfs gives
# a real ENOSPC without privileges. The repo is copied into the container, so its
# virtualenv doesn't clash with the host's.
set -eu
cd "$(dirname "$0")/.."
docker run --rm \
  --tmpfs /small:size=1m \
  -v "$PWD:/src:ro" \
  -v durastream-uv:/root/.cache/uv \
  -e DURASTREAM_SMALL_FS=/small \
  python:3.12-slim sh -euc '
    pip install -q --root-user-action=ignore uv
    mkdir /work
    tar -C /src --exclude=./.venv --exclude=./dist --exclude=__pycache__ \
      -cf - . | tar -xf - -C /work
    cd /work
    uv sync -q
    uv run pytest -q tests/test_disk_full.py
  '
