"""Post-process generated `_native.pyi` for mypy + stubtest compatibility.

Three transformations are applied via `libcst`, whose concrete syntax tree is
lossless — every unchanged token (crucially the native docstrings pyo3 0.29
emits, PR #5782, including PEP 224 attribute docstrings) is preserved exactly,
and only the nodes we rewrite change. `ruff format` runs after this script
(see the Makefile) to restore the project's stub formatting.

1. **Comparison-dunder calling convention** — pyo3 0.29 (PR #5841) emits
   `object` as the input *type* of rich-comparison dunders, but as
   `def __eq__(self, /, other: object) -> bool: ...` where `other` is
   positional-*or-keyword*. The CPython `tp_richcompare` slot is
   positional-*only*, so `stubtest` rejects that (though `mypy` accepts it).
   We fold the parameter into the positional-only group, renaming pyo3's
   `other` to the runtime's `value`.
2. **Bare-generic parametrization** — the introspection emits bare `list` for
   `Bound<PyList>`, bare `dict` for `Bound<PyDict>`, and bare `tuple` for
   `Bound<PyTuple>`. `mypy --strict` (`disallow_any_generics`) rejects bare
   generics, so we fill them (only in annotation position) with `Any`:
   `list[Any]`, `dict[Any, Any]`, `tuple[Any, ...]`. `Any` itself is left as-is
   — the config does not set `disallow_any_explicit`.
3. **`__all__`** — exported by the runtime but missing from the stub. We
   append (or replace) it, derived from the public top-level names.

Run after `stub_gen`, before any check that uses mypy.
"""

from __future__ import annotations

import sys
from pathlib import Path

import libcst as cst

# Rich-comparison dunders: their `tp_richcompare` slot is positional-only.
_SLOT_DUNDERS = frozenset({"__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__"})

# Bare builtin generics that must be parametrized for `mypy --strict`.
_BARE_GENERICS = frozenset({"list", "dict", "tuple"})


def _parametrized(name: str) -> cst.Subscript:
    """`list[Any]`, `dict[Any, Any]`, or `tuple[Any, ...]` as a CST node."""
    if name == "list":
        elements: list[cst.BaseExpression] = [cst.Name("Any")]
    elif name == "dict":
        elements = [cst.Name("Any"), cst.Name("Any")]
    else:  # tuple
        elements = [cst.Name("Any"), cst.Ellipsis()]
    return cst.Subscript(
        value=cst.Name(name),
        slice=[cst.SubscriptElement(slice=cst.Index(value=e)) for e in elements],
    )


def _fill(node: cst.BaseExpression) -> cst.BaseExpression:
    """Recursively parametrize bare generics inside an annotation expression.

    A `Subscript`'s value (an already-parametrized generic head) is left alone,
    but its slice is recursed into — so the inner bare `tuple` of
    `tuple[Any, tuple]` is caught.
    """
    if isinstance(node, cst.Name) and node.value in _BARE_GENERICS:
        return _parametrized(node.value)
    if isinstance(node, cst.Subscript):
        new_slice = [
            (
                element.with_changes(slice=element.slice.with_changes(value=_fill(element.slice.value)))
                if isinstance(element.slice, cst.Index)
                else element
            )
            for element in node.slice
        ]
        return node.with_changes(slice=new_slice)
    if isinstance(node, cst.BinaryOperation):  # `X | Y` unions
        return node.with_changes(left=_fill(node.left), right=_fill(node.right))
    if isinstance(node, (cst.Tuple, cst.List)):  # e.g. `Callable[[int, str], ...]`
        new_elements = [
            element.with_changes(value=_fill(element.value))
            if isinstance(element, cst.Element)
            else element
            for element in node.elements
        ]
        return node.with_changes(elements=new_elements)
    return node


class _StubTransformer(cst.CSTTransformer):
    def leave_Annotation(
        self, original_node: cst.Annotation, updated_node: cst.Annotation
    ) -> cst.Annotation:
        return updated_node.with_changes(annotation=_fill(updated_node.annotation))

    def leave_FunctionDef(
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef:
        if updated_node.name.value not in _SLOT_DUNDERS:
            return updated_node
        params = updated_node.params
        if not params.params:  # already positional-only -> nothing to move
            return updated_node
        moved = [
            p.with_changes(name=cst.Name("value")) if p.name.value == "other" else p
            for p in params.params
        ]
        # Normalize commas so the moved param + `/` render as `(self, value, /)`
        # with no trailing comma — a stray one triggers ruff's magic-comma
        # expansion into a multi-line signature.
        new_posonly = [
            p.with_changes(comma=cst.MaybeSentinel.DEFAULT)
            for p in (*params.posonly_params, *moved)
        ]
        new_params = params.with_changes(
            posonly_params=new_posonly, posonly_ind=cst.ParamSlash(), params=[]
        )
        return updated_node.with_changes(params=new_params)


def _final(annotation: cst.BaseExpression) -> bool:
    if isinstance(annotation, cst.Name):
        return annotation.value == "Final"
    return (
        isinstance(annotation, cst.Subscript)
        and isinstance(annotation.value, cst.Name)
        and annotation.value.value == "Final"
    )


def _public_names(module: cst.Module) -> list[str]:
    """Public top-level names, ordered to mirror the runtime `__all__`:
    `Final` constants, then classes, then functions, each in source order.
    """
    consts: list[str] = []
    classes: list[str] = []
    funcs: list[str] = []
    for stmt in module.body:
        if isinstance(stmt, cst.ClassDef):
            classes.append(stmt.name.value)
        elif isinstance(stmt, cst.FunctionDef):
            funcs.append(stmt.name.value)
        elif isinstance(stmt, cst.SimpleStatementLine):
            for small in stmt.body:
                if (
                    isinstance(small, cst.AnnAssign)
                    and isinstance(small.target, cst.Name)
                    and _final(small.annotation.annotation)
                ):
                    consts.append(small.target.value)

    ordered: list[str] = []
    seen: set[str] = set()
    for name in (*consts, *classes, *funcs):
        if name.startswith("__") and name != "__version__":
            continue
        if name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    return ordered


def _all_statement(names: list[str]) -> cst.SimpleStatementLine:
    elements = [cst.Element(value=cst.SimpleString(repr(n))) for n in names]
    assign = cst.Assign(
        targets=[cst.AssignTarget(target=cst.Name("__all__"))],
        value=cst.List(elements=elements),
    )
    return cst.SimpleStatementLine(body=[assign])


def _is_all_assignment(stmt: cst.BaseStatement) -> bool:
    if not isinstance(stmt, cst.SimpleStatementLine):
        return False
    return any(
        isinstance(small, cst.Assign)
        and any(isinstance(t.target, cst.Name) and t.target.value == "__all__" for t in small.targets)
        for small in stmt.body
    )


def _ensure_all(module: cst.Module) -> cst.Module:
    names = _public_names(module)
    if not names:
        return module
    new_stmt = _all_statement(names)
    body = list(module.body)
    for i, stmt in enumerate(body):
        if isinstance(stmt, cst.SimpleStatementLine) and _is_all_assignment(stmt):
            # Preserve blank lines / comments that preceded the old assignment.
            body[i] = new_stmt.with_changes(leading_lines=stmt.leading_lines)
            return module.with_changes(body=body)
    return module.with_changes(body=[*body, new_stmt])


def _typing_import(stmt: cst.BaseStatement) -> cst.ImportFrom | None:
    if not isinstance(stmt, cst.SimpleStatementLine):
        return None
    for small in stmt.body:
        if (
            isinstance(small, cst.ImportFrom)
            and isinstance(small.module, cst.Name)
            and small.module.value == "typing"
            and not isinstance(small.names, cst.ImportStar)
        ):
            return small
    return None


def _ensure_any_import(module: cst.Module) -> cst.Module:
    """Ensure `Any` is importable. No-op when it already is (the usual case,
    since pyo3 imports `Any` whenever it emits one)."""
    from libcst import matchers as m

    if not m.findall(module, m.Name("Any")):
        return module

    body = list(module.body)
    for i, stmt in enumerate(body):
        imp = _typing_import(stmt)
        if imp is None:
            continue
        assert not isinstance(imp.names, cst.ImportStar)  # narrowed in _typing_import
        if any(alias.name.value == "Any" for alias in imp.names):
            return module  # already imported
        new_imp = imp.with_changes(names=[*imp.names, cst.ImportAlias(name=cst.Name("Any"))])
        assert isinstance(stmt, cst.SimpleStatementLine)
        body[i] = stmt.with_changes(
            body=[new_imp if small is imp else small for small in stmt.body]
        )
        return module.with_changes(body=body)

    # No `from typing import ...` present: add one.
    new_line = cst.SimpleStatementLine(
        body=[cst.ImportFrom(module=cst.Name("typing"), names=[cst.ImportAlias(name=cst.Name("Any"))])]
    )
    return module.with_changes(body=[new_line, *body])


def transform(text: str) -> str:
    """Return the post-processed stub source for `text`. Pure; no file IO."""
    module = cst.parse_module(text)
    module = module.visit(_StubTransformer())
    module = _ensure_all(module)
    module = _ensure_any_import(module)
    return module.code


def fix(path: Path) -> bool:
    """Rewrite the stub in-place. Returns True if any change was made."""
    text = path.read_text(encoding="utf-8")
    new_text = transform(text)
    if new_text == text:
        return False
    path.write_text(new_text, encoding="utf-8")
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
