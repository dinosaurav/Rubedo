"""Selection language: key:value terms parsing to Selection."""

import pytest

from batchbrain import Selection


def test_reserved_prefixes():
    sel = Selection.parse(
        "source:folder:examples/input coord:*.txt step:extract "
        "version:1.2.0 live:true"
    )
    assert sel.source_id == "folder:examples/input"
    assert sel.coordinate_glob == "*.txt"
    assert sel.step == "extract"
    assert sel.code_version == "1.2.0"
    assert sel.invalidated is False


def test_open_vocabulary_is_the_index():
    sel = Selection.parse("company:acme domain:acme.com")
    assert sel.index == {"company": "acme", "domain": "acme.com"}


def test_quoted_values():
    sel = Selection.parse('company:"acme corp"')
    assert sel.index == {"company": "acme corp"}


def test_live_false_means_invalidated():
    assert Selection.parse("live:false").invalidated is True


def test_invalid_terms_raise():
    with pytest.raises(ValueError, match="expected key:value"):
        Selection.parse("just-a-word")
    with pytest.raises(ValueError, match="live: expects"):
        Selection.parse("live:maybe")


def test_empty_query_selects_everything():
    sel = Selection.parse("")
    assert sel == Selection()
