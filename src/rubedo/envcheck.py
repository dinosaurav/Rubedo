"""`rubedo check`: best-effort AST lint for pipeline(secrets=, env=) coverage.

Never imports the target file — same principle as server.py: read-only,
purely static. Walks the AST looking for `pipeline(...)` calls (to collect
the names declared in their `secrets=`/`env=` keyword arguments) and for
`@step`/`@source`-decorated functions (to find `os.environ[...]`/
`os.getenv(...)` reads inside their bodies whose variable name isn't among
the declared ones).

This is advisory *forever* (see notes/TODO.md item 21's Trap): anything
dynamic — a name built from a variable, an indirection through a helper
function two calls away, an aliased import — is silently skipped rather
than guessed at. Callers must never treat its output as authoritative or
use it to gate anything; it only ever produces warnings.
"""
import ast
from dataclasses import dataclass
from typing import List, Optional, Set


@dataclass(frozen=True)
class EnvWarning:
    """One undeclared `os.environ`/`os.getenv` read found inside a step."""
    step_name: str
    var_name: str


def _is_pipeline_call(func: ast.expr) -> bool:
    """`pipeline(...)` or `<anything>.pipeline(...)` (e.g. `rubedo.pipeline`)."""
    if isinstance(func, ast.Name):
        return func.id == "pipeline"
    if isinstance(func, ast.Attribute):
        return func.attr == "pipeline"
    return False


def _is_step_decorator(dec: ast.expr) -> bool:
    """`@step`, `@step(...)`, `@source`, `@source(...)`, or the bound-method
    forms `@p.step(...)`/`@p.source(...)` used on a `Pipeline` instance."""
    target = dec.func if isinstance(dec, ast.Call) else dec
    if isinstance(target, ast.Name):
        return target.id in ("step", "source")
    if isinstance(target, ast.Attribute):
        return target.attr in ("step", "source")
    return False


def _const_str(node: Optional[ast.expr]) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _string_list_literal(node: ast.expr) -> Set[str]:
    """A literal list/tuple of string constants -> that set of strings.
    Anything else (a variable, a comprehension, a call) -> empty: we never
    evaluate expressions, only read literals directly off the AST."""
    names: Set[str] = set()
    if isinstance(node, (ast.List, ast.Tuple)):
        for elt in node.elts:
            s = _const_str(elt)
            if s is not None:
                names.add(s)
    return names


def _is_environ_expr(node: ast.expr) -> bool:
    """`os.environ` (or any `<x>.environ`), or bare `environ` (from `from
    os import environ`)."""
    return (isinstance(node, ast.Attribute) and node.attr == "environ") or (
        isinstance(node, ast.Name) and node.id == "environ"
    )


def _is_os_expr(node: ast.expr) -> bool:
    return isinstance(node, ast.Name) and node.id == "os"


def _env_var_name(node: ast.AST) -> Optional[str]:
    """If `node` is `os.environ[...]`, `os.environ.get(...)`, or
    `os.getenv(...)` reading a string-literal name, return that name; else
    None (unrelated, or a dynamic name we don't trust)."""
    if isinstance(node, ast.Subscript) and _is_environ_expr(node.value):
        return _const_str(node.slice)
    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "getenv" and _is_os_expr(func.value):
            return _const_str(node.args[0]) if node.args else None
        if isinstance(func, ast.Name) and func.id == "getenv":
            return _const_str(node.args[0]) if node.args else None
        if isinstance(func, ast.Attribute) and func.attr == "get" and _is_environ_expr(func.value):
            return _const_str(node.args[0]) if node.args else None
    return None


def check_source(source: str, filename: str = "<unknown>") -> List[EnvWarning]:
    """Parse `source` and return undeclared `os.environ`/`os.getenv` reads
    found inside `@step`/`@source` function bodies.

    Returns [] on a syntax error in `source` — best-effort, never raises for
    a file that doesn't parse.
    """
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError:
        return []

    declared: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _is_pipeline_call(node.func):
            for kw in node.keywords:
                if kw.arg in ("secrets", "env") and kw.value is not None:
                    declared |= _string_list_literal(kw.value)

    warnings: List[EnvWarning] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not any(_is_step_decorator(d) for d in node.decorator_list):
            continue
        seen: Set[str] = set()
        for sub in ast.walk(node):
            var = _env_var_name(sub)
            if var is not None and var not in declared and var not in seen:
                seen.add(var)
                warnings.append(EnvWarning(step_name=node.name, var_name=var))
    return warnings


def check_file(path: str) -> List[EnvWarning]:
    """`check_source` over a file on disk. Raises OSError if `path` can't be
    read (that's a usage error for the caller to report, distinct from a
    parse failure inside the file, which `check_source` swallows)."""
    with open(path, "r") as f:
        source = f.read()
    return check_source(source, filename=path)
