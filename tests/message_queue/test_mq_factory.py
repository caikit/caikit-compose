"""
Test the MQ factory
"""
# Third Party
import pytest

# Local
from caikit_compose.message_queue import MQ_FACTORY
from caikit_compose.message_queue.local import LocalMessageQueue


def test_mq_factory_local():
    """Make sure the factory constructs a local mq correctly based on config"""
    inst = MQ_FACTORY.construct(
        {
            "type": LocalMessageQueue.name,
            "config": {
                "save_messages": True,
            },
        }
    )
    assert isinstance(inst, LocalMessageQueue)
    assert inst._save_messages


def test_mq_factory_invalid_type():
    """Make sure an invalid type raises"""
    with pytest.raises(ValueError):
        MQ_FACTORY.construct({"type": "FOOBAR"})
