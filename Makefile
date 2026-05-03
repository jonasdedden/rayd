# Common development tasks. Assumes uv is installed and a venv is set up via
# `make venv`.

VENV_BIN := .venv/bin
PYTHON := $(VENV_BIN)/python

.PHONY: venv build stubs stubs-only stubtest test lint check clean bench

venv:
	uv venv
	uv sync --group dev

build:
	uv run maturin develop --uv

# Regenerate `_native.pyi` from the *already-built* cdylib. Doesn't
# depend on `build` so CI (which builds explicitly in a separate
# step) can call this without re-invoking `maturin develop`. Local
# users want `make stubs`, which chains `build` + `stubs-only`.
#
# `cargo run --bin stub_gen` is intentionally NOT `--release`: the
# binary is invoked once per regeneration and reusing the dev-profile
# dep tree from the prior `maturin develop` saves ~50 s vs. building
# the workspace a second time in release mode.
#
# `.venv/bin/...` invocations bypass `uv run`'s editable-project
# resync, which would otherwise reinstall rayd between every step
# even though nothing changed.
stubs-only:
	RAYD_CDYLIB_PATH=$(shell .venv/bin/python -c 'import rayd._native, pathlib; print(rayd._native.__file__)') \
	    cargo run --bin stub_gen
	$(PYTHON) tools/fix_stubs.py python/rayd/_native.pyi
	# Splice runtime docstrings (from Rust doc comments captured as
	# `__doc__`) into the stub so IDEs and `help()` and the .pyi all
	# read from the same source. Must run AFTER fix_stubs (which uses
	# regex on bare `: ...` bodies) and BEFORE ruff format (which
	# normalises whitespace).
	$(PYTHON) tools/inject_docstrings.py python/rayd/_native.pyi
	# Format the regenerated stub so the committed copy is byte-
	# identical to a fresh regeneration — without this step the CI
	# stubs-freshness diff fails on whitespace-only deltas (multi-line
	# function signatures, `str | None` spacing, etc.) that pyo3-stub-gen
	# produces but `ruff format` would normalise.
	$(VENV_BIN)/ruff format python/rayd/_native.pyi

stubs: build stubs-only

test: build
	cargo test --workspace
	uv run pytest

lint:
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	uv run ruff format --check python
	uv run ruff check python
	uv run mypy

# Verify the committed `_native.pyi` matches the live `rayd._native`
# runtime types. Catches a regression of the per-function overrides
# in `tools/fix_stubs.py` — e.g. if PyO3 starts returning a different
# class shape than the .pyi advertises.
stubtest: build
	uv run python -m mypy.stubtest rayd._native

check: lint stubs stubtest test
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
