"""
Tests for the key grouping type
"""
# First Party
from caikit.interfaces.common.data_model import ProducerId
import aconfig

# Local
from caikit_compose.grouping.key_grouping import KeyGrouping
from caikit_compose.message import Message
from tests.conftest import Greeting, make_gs, make_message

## Helpers #####################################################################


def make_inst(grouping_input, group_store=None, subscription_id=None, **kwargs):
    gs = group_store or make_gs()
    sub_id = subscription_id or "some-subscription"
    return KeyGrouping(
        config=aconfig.Config(kwargs, override_env_vars=False),
        subscription_id=sub_id,
        group_store=gs,
        grouping_input=grouping_input,
    )


## Tests #######################################################################


def test_key_group_multi_content_type():
    """Test that multiple content types can be grouped by key"""
    inst = make_inst(
        grouping_input=[Greeting.full_name, ProducerId.full_name],
        match_keys=["metadata.source"],
    )
    greet1 = make_message("hi", metadata={"source": "1"})
    pid1 = Message.from_data(ProducerId("hi"), metadata={"source": "1"})
    greet2 = make_message("there", metadata={"source": "2"})
    pid2 = Message.from_data(ProducerId("there"), metadata={"source": "2"})

    # Add the first two messages that match on metadata.source to trigger the
    # first output group
    assert inst.add_message(greet1) is None
    res = inst.add_message(pid1)
    assert res
    assert list(res.unwrapped.messages) == [greet1, pid1]

    # Repeat and make sure the behavior is the same
    # NOTE: Simulating creation/arrival dates out of order
    assert inst.add_message(pid2) is None
    res = inst.add_message(greet2)
    assert res
    assert list(res.unwrapped.messages) == [pid2, greet2]


def test_key_grouping_no_conflicts():
    """Make sure that multiple key groupings use distinct group IDs and don't
    interfere with each other
    """
    gs = make_gs()
    inst1 = make_inst(
        group_store=gs,
        subscription_id="sub1",
        grouping_input=[Greeting.full_name, ProducerId.full_name],
    )
    inst2 = make_inst(
        group_store=gs,
        subscription_id="sub2",
        grouping_input=[Greeting.full_name, ProducerId.full_name],
    )
    data_id = "the-data"
    greet1 = make_message("hi", data_id=data_id)
    pid1 = Message.from_data(ProducerId("hi"), data_id=data_id)
    assert not inst1.add_message(greet1)
    assert not inst2.add_message(pid1)
    assert inst1.add_message(pid1)
    assert inst2.add_message(greet1)
