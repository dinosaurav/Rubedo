"""rubedo check: AST lint for pipeline(secrets=, env=) coverage — TODO 21.

Pure AST tests against `envcheck.check_source` (no file I/O, no import of
the analyzed code — the whole point of the tool). A separate test drives
`check_file` against a real file on disk, and another drives the CLI
end-to-end via `cli.main`.
"""
import sys

import pytest

from rubedo.envcheck import EnvWarning, check_source


def test_warns_on_undeclared_os_environ_subscript():
    src = """
from rubedo import pipeline, step

p = pipeline(name="x")

@p.step(name="a", version="1")
def a():
    import os
    return os.environ["OPENAI_API_KEY"]
"""
    warnings = check_source(src)
    assert warnings == [EnvWarning(step_name="a", var_name="OPENAI_API_KEY")]


def test_warns_on_undeclared_os_getenv():
    src = """
from rubedo import step

@step(name="a", version="1")
def a():
    import os
    return os.getenv("LOG_LEVEL")
"""
    warnings = check_source(src)
    assert warnings == [EnvWarning(step_name="a", var_name="LOG_LEVEL")]


def test_warns_on_os_environ_get():
    src = """
from rubedo import step

@step(name="a", version="1")
def a():
    import os
    return os.environ.get("LOG_LEVEL")
"""
    warnings = check_source(src)
    assert warnings == [EnvWarning(step_name="a", var_name="LOG_LEVEL")]


def test_passes_once_declared_in_secrets():
    src = """
from rubedo import pipeline, step

@step(name="a", version="1")
def a():
    import os
    return os.environ["OPENAI_API_KEY"]

pipeline(name="x", steps=[a], secrets=["OPENAI_API_KEY"])
"""
    assert check_source(src) == []


def test_passes_once_declared_in_env():
    src = """
from rubedo import pipeline, step

@step(name="a", version="1")
def a():
    import os
    return os.getenv("LOG_LEVEL")

pipeline(name="x", steps=[a], env=["LOG_LEVEL"])
"""
    assert check_source(src) == []


def test_ignores_reads_outside_step_bodies():
    """A module-level or helper-function read isn't inside a @step body —
    out of scope by design (dynamic indirection isn't traced)."""
    src = """
import os
from rubedo import step

MODEL = os.environ.get("SOME_MODEL", "default")

def _helper():
    return os.environ["HELPER_KEY"]

@step(name="a", version="1")
def a():
    return _helper()
"""
    assert check_source(src) == []


def test_ignores_dynamic_names():
    src = """
from rubedo import step

@step(name="a", version="1")
def a():
    import os
    key_name = "COMPUTED_" + "KEY"
    return os.environ[key_name]
"""
    assert check_source(src) == []


def test_skips_silently_on_parse_failure():
    assert check_source("def a(:\n  this is not python") == []


def test_dedupes_repeated_reads_of_same_var_in_one_step():
    src = """
from rubedo import step

@step(name="a", version="1")
def a():
    import os
    x = os.environ["KEY"]
    y = os.environ["KEY"]
    return x, y
"""
    warnings = check_source(src)
    assert warnings == [EnvWarning(step_name="a", var_name="KEY")]


def test_p_step_decorator_form_is_recognized():
    """The `p = pipeline(...); @p.step(...)` bound-decorator form used
    throughout examples/ (e.g. hn_digest.py), not just the bare `@step`."""
    src = """
from rubedo import pipeline

p = pipeline(name="x")

@p.step(name="a", version="1")
def a():
    import os
    yield {"key": os.environ["TOKEN"]}
"""
    warnings = check_source(src)
    assert warnings == [EnvWarning(step_name="a", var_name="TOKEN")]


def test_check_file_reads_from_disk(tmp_path):
    from rubedo.envcheck import check_file

    f = tmp_path / "pipe.py"
    f.write_text(
        "from rubedo import step\n"
        "@step(name='a', version='1')\n"
        "def a():\n"
        "    import os\n"
        "    return os.environ['UNDECLARED']\n"
    )
    warnings = check_file(str(f))
    assert warnings == [EnvWarning(step_name="a", var_name="UNDECLARED")]


def test_check_file_missing_raises_oserror(tmp_path):
    from rubedo.envcheck import check_file

    with pytest.raises(OSError):
        check_file(str(tmp_path / "does_not_exist.py"))


def test_cli_check_warns_then_passes_and_always_exits_zero(tmp_path, capsys, monkeypatch):
    """End-to-end through cli.main: warn-then-declare-then-pass, and the
    exit code never changes — the lint is advisory forever (TODO 21's Trap)."""
    from rubedo.cli import main

    undeclared = tmp_path / "undeclared.py"
    undeclared.write_text(
        "from rubedo import step\n"
        "@step(name='a', version='1')\n"
        "def a():\n"
        "    import os\n"
        "    return os.environ['MY_SECRET']\n"
    )

    monkeypatch.setattr(sys, "argv", ["rubedo", "check", str(undeclared)])
    main()
    out = capsys.readouterr().out
    assert "MY_SECRET" in out
    assert "warning" in out.lower()

    declared = tmp_path / "declared.py"
    declared.write_text(
        "from rubedo import pipeline, step\n"
        "@step(name='a', version='1')\n"
        "def a():\n"
        "    import os\n"
        "    return os.environ['MY_SECRET']\n"
        "pipeline(name='x', steps=[a], secrets=['MY_SECRET'])\n"
    )
    monkeypatch.setattr(sys, "argv", ["rubedo", "check", str(declared)])
    main()
    out = capsys.readouterr().out
    assert "no undeclared" in out.lower()
