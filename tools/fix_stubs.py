"""Post-process generated `_native.pyi` for mypy + stubtest compatibility.

Three groups of fixes are applied:

1. **Liskov fixup** — PyO3 0.28's introspection emits `__eq__`/`__ne__`
   with the concrete pyclass type and the parameter named `other`, but the
   runtime implementation accepts `(self, value, /)`. We rewrite to
   `def __eq__(self, value: object, /) -> bool: ...`.
2. **Generics tightening** — the introspection emits `Any` for `Bound<PyAny>`,
   `list` for `Bound<PyList>`, `dict` for `Bound<PyDict>`, and `tuple` for
   `Bound<PyTuple>`. With `mypy --strict` (and our project's ban on `Any` in
   typed code), those bare generics fail. We tighten to `object`,
   `list[object]`, `dict[object, object]`, and `tuple[object, ...]`.
3. **`__all__`** — exported by the runtime but missing from the stub. We
   append it, derived from the public top-level names.

Run after `stub_gen`, before any check that uses mypy.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# (1) Liskov fixup
_DUNDER_RE = re.compile(
    r"def (__eq__|__ne__)\(self, /, other: [^)]+\) -> bool: \.\.\.",
)

# (2a) Replace the PyO3 stub's `Any` with `object`. We do this with a token
# match so we don't touch unrelated identifiers.
_ANY_TOKEN_RE = re.compile(r"\bAny\b")

# (2b) Tighten bare generics in stub function signatures.
#
# Inputs use covariant abstract types (`Sequence`, `Mapping`) so a typed
# `list[ObjectRef]` can be passed to a parameter annotated as
# `Sequence[object]` without tripping mypy's invariance.
# Returns use concrete types (`list[object]`, `dict[object, object]`,
# `tuple[object, ...]`) — variance isn't an issue on the way out.
_PARAM_LIST_RE = re.compile(r"(:\s*)list\b(?!\[)")
_RET_LIST_RE = re.compile(r"(->\s*)list\b(?!\[)")
_PARAM_DICT_RE = re.compile(r"(:\s*)dict\b(?!\[)")
_RET_DICT_RE = re.compile(r"(->\s*)dict\b(?!\[)")
_PARAM_TUPLE_RE = re.compile(r"(:\s*)tuple\b(?!\[)")
_RET_TUPLE_RE = re.compile(r"(->\s*)tuple\b(?!\[)")
# Bare `tuple` nested inside another generic, e.g. `tuple[object, tuple]`
# from `__reduce__`'s outer-tuple-of-(callable, args) signature.
_NESTED_BARE_TUPLE_RE = re.compile(r"(,\s*)tuple\b(?!\[)")
_NEEDS_SEQUENCE_IMPORT_RE = re.compile(r"\bSequence\[")
_NEEDS_MAPPING_IMPORT_RE = re.compile(r"\bMapping\[")
_HAS_SEQUENCE_IMPORT_RE = re.compile(r"^from collections\.abc import .*\bSequence\b", re.MULTILINE)
_HAS_MAPPING_IMPORT_RE = re.compile(r"^from collections\.abc import .*\bMapping\b", re.MULTILINE)
_COLLECTIONS_ABC_LINE_RE = re.compile(r"^from collections\.abc import (.+)$", re.MULTILINE)

# (3) Top-level declarations.
_CLASS_RE = re.compile(r"^class (\w+)[\s(:]", re.MULTILINE)
_FUNC_RE = re.compile(r"^def (\w+)\(", re.MULTILINE)
_CONST_RE = re.compile(r"^(\w+)\s*:\s*Final\b", re.MULTILINE)
_ALL_RE = re.compile(r"^__all__\s*=", re.MULTILINE)
_ALL_RE_FULL = re.compile(r"^__all__\s*=\s*\[[^\]]*\]", re.MULTILINE)

def _rewrite_dunders(text: str) -> str:
    return _DUNDER_RE.sub(
        lambda m: f"def {m.group(1)}(self, value: object, /) -> bool: ...",
        text,
    )


def _tighten_generics(text: str) -> str:
    text = _ANY_TOKEN_RE.sub("object", text)
    text = _PARAM_LIST_RE.sub(r"\1Sequence[object]", text)
    text = _RET_LIST_RE.sub(r"\1list[object]", text)
    text = _PARAM_DICT_RE.sub(r"\1Mapping[object, object]", text)
    text = _RET_DICT_RE.sub(r"\1dict[object, object]", text)
    text = _PARAM_TUPLE_RE.sub(r"\1tuple[object, ...]", text)
    text = _RET_TUPLE_RE.sub(r"\1tuple[object, ...]", text)
    text = _NESTED_BARE_TUPLE_RE.sub(r"\1tuple[object, ...]", text)
    # Convention: a parameter named `kwargs` always has string keys in Python.
    # Narrow so callers can pass `dict[str, object]` directly.
    text = re.sub(
        r"kwargs:\s*Mapping\[object, object\]",
        "kwargs: Mapping[str, object]",
        text,
    )
    return _ensure_collections_imports(text)


def _ensure_collections_imports(text: str) -> str:
    needs_sequence = bool(_NEEDS_SEQUENCE_IMPORT_RE.search(text))
    needs_mapping = bool(_NEEDS_MAPPING_IMPORT_RE.search(text))
    if not (needs_sequence or needs_mapping):
        return text

    line_match = _COLLECTIONS_ABC_LINE_RE.search(text)
    if line_match is None:
        # No existing collections.abc import; insert one at the top.
        wanted: list[str] = []
        if needs_mapping:
            wanted.append("Mapping")
        if needs_sequence:
            wanted.append("Sequence")
        new_line = f"from collections.abc import {', '.join(wanted)}\n"
        return new_line + text

    existing = {n.strip() for n in line_match.group(1).split(",")}
    if needs_sequence:
        existing.add("Sequence")
    if needs_mapping:
        existing.add("Mapping")
    new_line = f"from collections.abc import {', '.join(sorted(existing))}"
    start, end = line_match.span()
    return text[:start] + new_line + text[end:]


def _drop_any_from_imports(text: str) -> str:
    """Remove `Any` from `from typing import ...` lines.

    Idempotent. Run *before* the body-level `Any` → `object` rewrite, so the
    rewrite doesn't accidentally produce `from typing import object, Final`.
    """
    pattern_leading = re.compile(r"^(from typing import )Any,\s*", re.MULTILINE)
    pattern_middle = re.compile(r"^(from typing import .+?),\s*Any(?=[,\s])", re.MULTILINE)
    pattern_trailing = re.compile(r"^(from typing import .+?),\s*Any$", re.MULTILINE)
    pattern_only = re.compile(r"^from typing import Any$\n", re.MULTILINE)

    text = pattern_only.sub("", text)
    text = pattern_leading.sub(r"\1", text)
    text = pattern_middle.sub(r"\1", text)
    text = pattern_trailing.sub(r"\1", text)
    return text


def _ensure_all(text: str) -> str:
    """Rewrite or append `__all__` to match the stub's actual top-level names.

    PyO3 0.28 auto-fills `__all__` at runtime including underscore-prefixed
    entries (e.g. test helpers like `_pool_pending`); pyo3-introspection's
    emitted stub may omit them. Always regenerating keeps stubtest happy.
    """
    names: list[str] = []
    seen: set[str] = set()
    # Constants first (matches __version__'s spot in the runtime list).
    for regex in (_CONST_RE, _CLASS_RE, _FUNC_RE):
        for match in regex.finditer(text):
            name = match.group(1)
            # Dunder names (e.g. `__getattr__`) should never be in __all__.
            if name.startswith("__") and name != "__version__":
                continue
            if name in seen:
                continue
            seen.add(name)
            names.append(name)
    if not names:
        return text
    formatted = ", ".join(repr(n) for n in names)
    new_all = f"__all__ = [{formatted}]"

    if _ALL_RE.search(text):
        return _ALL_RE_FULL.sub(new_all, text, count=1)
    return f"{text.rstrip()}\n\n{new_all}\n"


def fix(path: Path) -> bool:
    """Rewrite the stub in-place. Returns True if any change was made."""
    text = path.read_text(encoding="utf-8")
    text2 = _rewrite_dunders(text)
    text2 = _drop_any_from_imports(text2)
    text2 = _tighten_generics(text2)
    text2 = _ensure_all(text2)
    if text2 == text:
        return False
    path.write_text(text2, encoding="utf-8")
    return True


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: fix_stubs.py <path-to-pyi> [<path-to-pyi> ...]", file=sys.stderr)
        return 2
    for arg in argv[1:]:
        path = Path(arg)
        if not path.exists():
            print(f"fix_stubs: skipping nonexistent {path}", file=sys.stderr)
            continue
        if fix(path):
            print(f"fix_stubs: patched {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
