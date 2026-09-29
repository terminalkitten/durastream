.PHONY: full check lint format typecheck test test-diskfull bench build demo demo-bench demo-restart demo-queue demo-ledger demo-concurrent demo-serve present-preview present-build

# everything, in order: fix formatting, lint + test, benchmark, build dist/
full:
	$(MAKE) format
	$(MAKE) check
	$(MAKE) bench
	$(MAKE) build

# everything CI checks
check: lint test

# check only, no edits
lint: typecheck
	uv run ruff check .
	uv run ruff format --check .

# apply fixes
format:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run ty check

test:
	uv run pytest -q

# disk-full rollback against a real ENOSPC (tiny tmpfs, in Docker)
test-diskfull:
	scripts/test-diskfull.sh

# LLM -> durastream -> SSE workload: token latency, CPU, resume
bench:
	uv run python scripts/bench_duplex.py

# wheel + sdist into dist/
build:
	uv build --out-dir dist

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
