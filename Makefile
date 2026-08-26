.PHONY: lint typecheck format test demo demo-bench demo-restart demo-queue demo-ledger demo-serve present-preview present-build

lint: format typecheck

format:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run ty check

test:
	uv run python tests/test_durastream.py

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

demo-serve:
	uv run python demos/fastapi_resume.py

