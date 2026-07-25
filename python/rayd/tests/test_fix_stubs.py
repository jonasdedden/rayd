"""Unit tests for `tools/fix_stubs.py` (importable via pytest `pythonpath`).

These exercise the pure `transform()` on small stub snippets — no compiled
extension or file IO needed — covering each of the three transformations plus
the invariants that make the AST-driven approach safe (docstrings preserved,
non-annotation identifiers untouched, idempotency).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import fix_stubs

if TYPE_CHECKING:
    from pathlib import Path

_COMPARISON_DUNDERS = ("__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__")


def test_comparison_dunders_made_positional_only() -> None:
    for dunder in _COMPARISON_DUNDERS:
        src = f"class C:\n    def {dunder}(self, /, other: object) -> bool: ...\n"
        out = fix_stubs.transform(src)
        assert f"def {dunder}(self, value: object, /) -> bool: ..." in out


def test_dunder_already_positional_only_is_unchanged() -> None:
    line = "    def __eq__(self, value: object, /) -> bool: ..."
    out = fix_stubs.transform(f"class C:\n{line}\n")
    assert line in out


def test_non_comparison_dunder_untouched() -> None:
    # __hash__ is not a rich-comparison slot; its signature must be left alone.
    line = "    def __hash__(self, /) -> int: ..."
    out = fix_stubs.transform(f"class C:\n{line}\n")
    assert line in out


def test_bare_generics_filled_in_params_and_returns() -> None:
    src = "def f(a: list, b: dict, c: tuple) -> list: ...\n"
    out = fix_stubs.transform(src)
    assert "a: list[Any]" in out
    assert "b: dict[Any, Any]" in out
    assert "c: tuple[Any, ...]" in out
    assert "-> list[Any]" in out


def test_nested_bare_tuple_is_filled() -> None:
    src = "from typing import Any\ndef f() -> tuple[int, tuple]: ...\n"
    out = fix_stubs.transform(src)
    assert "-> tuple[int, tuple[Any, ...]]" in out


def test_already_parametrized_generic_untouched() -> None:
    src = "from typing import Any\ndef f() -> list[int]: ...\n"
    out = fix_stubs.transform(src)
    assert "-> list[int]:" in out
    assert "list[Any]" not in out


def test_base_class_identifier_not_filled() -> None:
    # `list` as a base class is not an annotation position and must be left as-is.
    out = fix_stubs.transform("class C(list): ...\n")
    assert "class C(list):" in out
    assert "list[Any]" not in out


def test_union_annotation_filled() -> None:
    src = "from typing import Any\ndef f() -> list | None: ...\n"
    out = fix_stubs.transform(src)
    assert "-> list[Any] | None:" in out


def test_all_ordering_and_filtering() -> None:
    src = (
        "from typing import Final\n"
        "__version__: Final[str]\n"
        "class Beta: ...\n"
        "class Alpha: ...\n"
        "def _private() -> None: ...\n"
        "def public() -> None: ...\n"
        "def __dunder__() -> None: ...\n"
    )
    out = fix_stubs.transform(src)
    # Constants first, then classes, then functions — each in source order.
    # `_private` is kept (exported at runtime); `__dunder__` is dropped.
    assert "__all__ = ['__version__', 'Beta', 'Alpha', '_private', 'public']" in out


def test_existing_all_is_replaced() -> None:
    out = fix_stubs.transform("class C: ...\n__all__ = ['stale', 'gone']\n")
    assert "__all__ = ['C']" in out
    assert "stale" not in out


def test_attribute_docstring_preserved_verbatim() -> None:
    src = (
        "from typing import Final\n"
        "class C:\n"
        "    X: Final[int]\n"
        '    """\n    Attribute doc.\n    """\n'
        "    def __eq__(self, /, other: object) -> bool: ...\n"
    )
    out = fix_stubs.transform(src)
    # PEP 224 attribute docstring stays triple-quoted (not escaped by unparse).
    assert '"""\n    Attribute doc.\n    """' in out
    assert "def __eq__(self, value: object, /) -> bool: ..." in out


def test_any_import_added_when_missing() -> None:
    out = fix_stubs.transform("def f() -> list: ...\n")
    assert "from typing import Any" in out
    assert out.count("import Any") == 1


def test_any_import_not_duplicated_when_present() -> None:
    src = "from typing import Any, Final\ndef f() -> list: ...\n"
    out = fix_stubs.transform(src)
    assert out.count("from typing import") == 1


def test_clean_stub_is_returned_unchanged() -> None:
    src = "def f() -> int: ...\n\n__all__ = ['f']\n"
    assert fix_stubs.transform(src) == src


def test_fix_writes_and_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "m.pyi"
    path.write_text("def f() -> list: ...\n", encoding="utf-8")
    assert fix_stubs.fix(path) is True
    written = path.read_text(encoding="utf-8")
    assert "-> list[Any]" in written
    assert "from typing import Any" in written
    # A second pass has nothing left to change.
    assert fix_stubs.fix(path) is False
