"""
Unit tests for utils.dict_tools
"""

# Third Party
# Thrid Party
import pytest

# Local
from caikit_compose.utils import dict_tools


@pytest.mark.parametrize(
    ["base", "overrides", "expected"],
    [
        (
            {"foo": 1, "bar": 2},
            {"foo": 10, "baz": "asdf"},
            {"foo": 10, "bar": 2, "baz": "asdf"},
        ),
        (
            {"top": {"foo": 1, "bar": 2}},
            {"top": {"foo": 10, "baz": "asdf"}},
            {"top": {"foo": 10, "bar": 2, "baz": "asdf"}},
        ),
    ],
)
def test_deep_merge(base, overrides, expected):
    res = dict_tools.deep_merge(base, overrides)
    assert res == expected
    assert base == expected
