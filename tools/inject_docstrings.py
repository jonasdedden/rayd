"""Splice runtime docstrings into the generated `_native.pyi` stub.

`pyo3-introspection` (used by `cargo run --bin stub_gen`) emits typed
signatures but no docstrings. The Rust doc comments on
`#[pyfunction]` / `#[pymethods]` items ARE captured at runtime as
`__doc__`, so this script imports the live module, walks its public
attributes, and rewrites the stub so each function / class / method
that has a docstring carries it in the .pyi too.

Result: IDE hover + `help()` + static type-checkers all hit the
same single source — the .pyi — for both signature and prose.

Usage:
    python3 tools/inject_docstrings.py python/rayd/_native.pyi

Run AFTER `tools/fix_stubs.py` so the regex tightening is already
applied to the body we're about to rewrite.
"""

from __future__ import annotations

import argparse
import importlib
import pathlib
import re
import sys
import textwrap

_MODULE = "rayd._native"

# Start of a `def` declaration — indent + name. Body may span lines.
_DEF_START_RE = re.compile(r"^(?P<indent>\s*)def (?P<name>\w+)\(")

# Trailing stub body marker. We only splice when the def's last line
# ends with `: ...` (which `pyo3-introspection` always emits).
_DEF_END_RE = re.compile(r": \.\.\.$")

# Top-level class header. Captures the class name and tolerates the
# `(...)` base list and trailing `:`.
_CLASS_LINE_RE = re.compile(r"^class (?P<name>\w+)[(:]")


def collect_docs(module_name: str) -> dict[str, str]:
    """Return `{qualname: __doc__}` for every documented public symbol.

    `qualname` is `name` for top-level entries and `Class.member` for
    class members. Empty / whitespace-only docstrings are skipped so
    the output diff stays minimal.
    """
    mod = importlib.import_module(module_name)
    docs: dict[str, str] = {}

    # Cache of `object.<dunder>.__doc__` so we can filter inherited
    # docstrings. PyO3-generated classes inherit `object.__new__` /
    # `object.__init__` whose docstrings ("Create and return a new
    # object...") aren't user-authored content and would just add noise.
    inherited_docs = {
        name: (getattr(object, name, None).__doc__ or "")
        for name in dir(object)
        if getattr(object, name, None) is not None
    }

    def add(qualname: str, obj: object, member_name: str | None = None) -> None:
        doc = getattr(obj, "__doc__", None)
        if not isinstance(doc, str) or not doc.strip():
            return
        # Filter out docstrings inherited verbatim from `object`.
        if member_name is not None and inherited_docs.get(member_name) == doc:
            return
        docs[qualname] = doc

    for name in dir(mod):
        # Skip private + dunders at module level. `__version__` is a
        # `Final[str]` constant; `Final` typed assignments don't take
        # a docstring in stubs.
        if name.startswith("_"):
            continue
        obj = getattr(mod, name)
        add(name, obj)
        if isinstance(obj, type):
            for mname in dir(obj):
                # Skip auto-generated dunders other than the constructor
                # pair (where Rust-side docs often live).
                if mname.startswith("__") and mname not in {"__init__", "__new__"}:
                    continue
                if mname.startswith("_") and not mname.startswith("__"):
                    continue
                member = getattr(obj, mname, None)
                if member is None:
                    continue
                add(f"{name}.{mname}", member, member_name=mname)
    return docs


def _format_block(doc: str, indent: str) -> str:
    """Render `doc` as a triple-quoted block, all lines prefixed with `indent`.

    - Trailing whitespace is trimmed.
    - Leading common whitespace (typical of multi-line docstrings) is
      dedented before re-indenting under `indent`.
    - Inner triple-quotes are escaped (rare in Rust doc comments but
      we don't want to silently corrupt them).
    """
    text = doc.rstrip().replace('"""', '\\"\\"\\"')
    # First line of a docstring is often un-indented even when the
    # rest is — `textwrap.dedent` handles that gracefully when only
    # the second-and-later lines have a common indent.
    text = textwrap.dedent(text)
    lines = text.splitlines()
    if not lines:
        return f'{indent}""""""'
    if len(lines) == 1:
        return f'{indent}"""{lines[0]}"""'
    rest = "\n".join((indent + ln) if ln.strip() else "" for ln in lines[1:])
    return f'{indent}"""{lines[0]}\n{rest}\n{indent}"""'


def inject(stub_text: str, docs: dict[str, str]) -> str:
    """Walk the stub line-by-line, splicing docstrings in place.

    Handles single-line defs like
        def foo(x: int) -> int: ...
    and multi-line defs whose signature wraps across several lines:
        def __new__(
            cls, /, ...
        ) -> Foo: ...
    by buffering the def lines until we see the trailing `: ...`.
    """
    lines = stub_text.splitlines(keepends=True)
    out: list[str] = []
    current_class: str | None = None
    i = 0
    while i < len(lines):
        raw_line = lines[i]
        line = raw_line.rstrip("\n")
        stripped = line.lstrip()

        # Class header → emit, then optionally emit the class docstring.
        m_class = _CLASS_LINE_RE.match(line)
        if m_class:
            current_class = m_class.group("name")
            out.append(raw_line)
            doc = docs.get(current_class)
            if doc:
                out.append(_format_block(doc, "    ") + "\n")
            i += 1
            continue

        # Top-level non-blank, non-decorator line → class scope ends.
        if stripped and not line[0].isspace() and not stripped.startswith("@"):
            current_class = None

        # Def start? Consume lines until the trailing `: ...`. Bail
        # gracefully if we reach EOF without finding it.
        m_def = _DEF_START_RE.match(line)
        if m_def:
            indent = m_def.group("indent")
            name = m_def.group("name")
            j = i
            while j < len(lines) and not _DEF_END_RE.search(
                lines[j].rstrip("\n")
            ):
                j += 1
            if j >= len(lines):
                # Malformed def — emit verbatim and move on.
                out.append(raw_line)
                i += 1
                continue
            sig_block = lines[i : j + 1]
            qualname = f"{current_class}.{name}" if current_class else name
            doc = docs.get(qualname)
            if doc:
                inner = indent + "    "
                # The last line ends with `: ...`; strip the `...` body
                # marker, keep the colon, then add the docstring + an
                # explicit `...` body line.
                last = sig_block[-1].rstrip("\n")
                last = _DEF_END_RE.sub(":", last)
                rewritten = sig_block[:-1] + [last + "\n"]
                out.extend(rewritten)
                out.append(_format_block(doc, inner) + "\n")
                out.append(f"{inner}...\n")
            else:
                out.extend(sig_block)
            i = j + 1
            continue

        out.append(raw_line)
        i += 1

    return "".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "stub",
        type=pathlib.Path,
        help="Path to the .pyi file to rewrite (e.g. python/rayd/_native.pyi)",
    )
    parser.add_argument(
        "--module",
        default=_MODULE,
        help=f"Importable module to walk (default: {_MODULE})",
    )
    args = parser.parse_args(argv)

    if not args.stub.exists():
        print(f"inject_docstrings: stub not found: {args.stub}", file=sys.stderr)
        return 1

    docs = collect_docs(args.module)
    text = args.stub.read_text(encoding="utf-8")
    new_text = inject(text, docs)
    if new_text == text:
        print(f"inject_docstrings: no change to {args.stub}")
        return 0
    args.stub.write_text(new_text, encoding="utf-8")
    print(f"inject_docstrings: spliced {len(docs)} docstrings into {args.stub}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
