"""
Test the MQ factory
"""
# Third Party
import pytest

# Local
from caikit_compose.group_store import GROUP_STORE_FACTORY
from caikit_compose.group_store.local import LocalGroupStore


def test_gs_factory_local():
    """Make sure the factory constructs a local mq correctly based on config"""
    inst = GROUP_STORE_FACTORY.construct(
        {
            "type": LocalGroupStore.name,
            "config": {
                "save_messages": True,
            },
        }
    )
    assert isinstance(inst, LocalGroupStore)


def test_mq_factory_invalid_type():
    """Make sure an invalid type raises"""
    with pytest.raises(ValueError):
        GROUP_STORE_FACTORY.construct({"type": "FOOBAR"})
