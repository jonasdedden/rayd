# Common development tasks. Assumes uv is installed and a venv is set up via
# `make venv`.

VENV_BIN := .venv/bin
PYTHON := $(VENV_BIN)/python

.PHONY: venv build stubs test lint check clean bench

venv:
	uv venv
	uv sync --group dev

build:
	uv run maturin develop --uv

stubs: build
	RAYD_CDYLIB_PATH=$(shell .venv/bin/python -c 'import rayd._native, pathlib; print(rayd._native.__file__)') \
	    cargo run --bin stub_gen --release
	$(PYTHON) tools/fix_stubs.py python/rayd/_native.pyi
	# Format the regenerated stub so the committed copy is byte-
	# identical to a fresh regeneration — without this step the CI
	# stubs-freshness diff fails on whitespace-only deltas (multi-line
	# function signatures, `str | None` spacing, etc.) that pyo3-stub-gen
	# produces but `ruff format` would normalise.
	uv run ruff format python/rayd/_native.pyi

test: build
	cargo test --workspace
	uv run pytest

lint:
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	uv run ruff format --check python
	uv run ruff check python
	uv run mypy

check: lint stubs test
	@echo "all checks green."

bench: build
	@echo "==> task throughput"
	uv run python -m rayd.benches.bench_task_throughput
	@echo "==> task latency"
	uv run python -m rayd.benches.bench_task_latency
	@echo "==> put/get bandwidth"
	uv run python -m rayd.benches.bench_put_get

clean:
	cargo clean
	rm -rf .venv .mypy_cache .ruff_cache .pytest_cache
	find . -name '__pycache__' -type d -exec rm -rf {} +
