"""
Common test setup and utils
"""

# Standard
import tempfile
import time

# Third Party
import pytest

# First Party
from caikit.core import DataObjectBase, dataobject
from caikit.core.toolkit import logging

# Local
from caikit_compose.group_store import GROUP_STORE_FACTORY
from caikit_compose.message import Message
from caikit_compose.message_queue import MQ_FACTORY

logging.configure()


@pytest.fixture
def workdir() -> str:
    with tempfile.TemporaryDirectory() as workdir:
        yield workdir


@dataobject
class Greeting(DataObjectBase):
    greeting: str


def make_message(greeting: str, **header_kwargs) -> Message:
    return Message.from_data(Greeting(greeting), **header_kwargs)


def make_gs():
    return GROUP_STORE_FACTORY.construct({"type": "LOCAL"})


def make_mq(**kwargs):
    return MQ_FACTORY.construct({"type": "LOCAL", "config": kwargs})


def drain_local_mq(mq):
    """Block until local MQ has handled all published messages"""
    # Wait a little to make sure the submission has initialized
    time.sleep(0.01)
    while not mq._pool._work_queue.empty():
        time.sleep(0.001)
