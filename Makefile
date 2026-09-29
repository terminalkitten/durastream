.PHONY: full check lint format typecheck test test-rust test-native test-pure test-diskfull bench build demo demo-bench demo-restart demo-queue demo-ledger demo-concurrent demo-serve present-preview present-build

CORE := --manifest-path core/Cargo.toml

# everything, in order: fix formatting, lint + test, benchmark, build dist/
full:
	$(MAKE) format
	$(MAKE) check
	$(MAKE) bench
	$(MAKE) build

# everything CI checks
check: lint test

# check only, no edits: Python + Rust
lint: typecheck
	uv run ruff check .
	uv run ruff format --check .
	cargo fmt $(CORE) --check
	cargo clippy $(CORE) --all-targets --features python -- -D warnings

# apply fixes: Python + Rust
format:
	uv run ruff check --fix .
	uv run ruff format .
	cargo fmt $(CORE)

typecheck:
	uv run ty check

test: test-rust test-native test-pure

test-rust:
	cargo test $(CORE)

# pytest per engine; each first asserts the engine really loaded, so a broken
# native build can't silently fall back and pass as pure Python.
# uv run rebuilds the extension when Rust sources change.
test-native:
	uv run python -c "import durastream as d; assert d.ENGINE == 'native', d.ENGINE"
	uv run pytest -q

test-pure:
	DURASTREAM_PURE=1 uv run python -c "import durastream as d; assert d.ENGINE == 'python', d.ENGINE"
	DURASTREAM_PURE=1 uv run pytest -q

# disk-full rollback against a real ENOSPC (tiny tmpfs, in Docker)
test-diskfull:
	scripts/test-diskfull.sh

bench:
	uv run python scripts/bench.py

# into dist/: Rust wheel (this platform only), sdist, pure-Python fallback wheel
build:
	uvx maturin build --release $(CORE) --out dist
	uvx maturin sdist $(CORE) --out dist
	uvx hatchling build -t wheel -d dist

demo:
	uv run python demos/bulk_stream.py

demo-bench:
	uv run python demos/append_vs_batch.py

demo-restart:  # optional args: make demo-restart 100 50
	uv run python demos/restart_stream.py $(filter-out $@,$(MAKECMDGOALS))

demo-queue:
	uv run python demos/work_queue.py

demo-ledger:
	uv run python demos/ledger.py

demo-concurrent:
	uv run python demos/concurrent_users.py

demo-serve:
	uv run python demos/fastapi_resume.py

# swallow trailing positional args (e.g. `make demo-restart 100 50`)
%:
	@:
