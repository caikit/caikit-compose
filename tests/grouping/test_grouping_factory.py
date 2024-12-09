"""
Tests for the grouping factory
"""

# Standard
from typing import List

# Local
from caikit_compose.grouping import GROUPING_FACTORY, GroupingBase
from caikit_compose.grouping.factory import GroupingFactory
from caikit_compose.grouping.individual_grouping import IndividualGrouping
from caikit_compose.message import Message
from tests.conftest import make_gs

## Helpers #####################################################################


class DemoGrouping(GroupingBase):

    name = "DEMO"

    def __init__(self, config, subscription_id, group_store, grouping_input):
        self.num_msgs = config.num_msgs or 2
        self.subscription_id = subscription_id
        self.group_store = group_store
        self.grouping_input = grouping_input

    def add_message(self, message):
        """STUB"""

    def get_pending_messages(self) -> List[Message]:
        return []


## Tests #######################################################################


def test_register_and_use():
    """Make sure that a new grouping type can be registered and used"""
    fcty = GroupingFactory("demo")
    fcty.register(DemoGrouping)
    sub_id = "1234"
    gs = make_gs()
    inst = fcty.construct(
        {"type": DemoGrouping.name, "config": {"num_msgs": 10}},
        sub_id,
        gs,
        "some-content-type",
    )
    assert isinstance(inst, DemoGrouping)
    assert inst.num_msgs == 10
    assert inst.subscription_id == sub_id
    assert inst.group_store is gs
    assert inst.grouping_input == ["some-content-type"]


def test_individual_grouping():
    """Make sure an individual grouping can be constructed"""
    gs = make_gs()
    inst = GROUPING_FACTORY.construct(
        {"type": IndividualGrouping.name}, "subid", gs, ["some-content-type"]
    )
    assert isinstance(inst, IndividualGrouping)
