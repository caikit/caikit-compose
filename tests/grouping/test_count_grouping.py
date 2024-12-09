"""
Tests for the count grouping type
"""
# Standard
from unittest.mock import MagicMock

# First Party
from caikit.interfaces.common.data_model import ProducerId
import aconfig

# Local
from caikit_compose.grouping.compound_grouping_base import GroupElement
from caikit_compose.grouping.count_grouping import CountGrouping
from caikit_compose.message import Message
from tests.conftest import Greeting, make_gs, make_message

## Helpers #####################################################################


def make_inst(grouping_input, **kwargs):
    gs = make_gs()
    sub_id = "some-subscription"
    return CountGrouping(
        config=aconfig.Config(kwargs, override_env_vars=False),
        subscription_id=sub_id,
        group_store=gs,
        grouping_input=grouping_input,
    )


def check_series(res, expected_elements):
    assert res
    assert len(res.unwrapped.series) == len(expected_elements)
    for res_elt, exp_elt in zip(res.unwrapped.series, expected_elements):
        assert len(res_elt.messages) == len(exp_elt)
        assert list(res_elt.messages) == exp_elt


## Tests #######################################################################


def test_counting_group_single_content_type():
    """Test that a single message content type can be grouped by count"""
    inst = make_inst(window_size=2, grouping_input=[Greeting.full_name])
    msg1 = make_message("hi")
    msg2 = make_message("there")
    msg3 = make_message("world")

    # Add a single message and make sure it does not trigger a completed group
    assert inst.add_message(msg1) is None

    # Add a second message and make sure it triggers a completed group
    check_series(inst.add_message(msg2), [[msg1], [msg2]])

    # Add a third message and make sure the window slid by 1
    check_series(inst.add_message(msg3), [[msg2], [msg3]])


def test_counting_group_window_stride():
    """Test that a window stride greater than 1 works as expected"""
    inst = make_inst(
        window_size=2, window_stride=2, grouping_input=[Greeting.full_name]
    )
    msg1 = make_message("hi")
    msg2 = make_message("there")
    msg3 = make_message("world")
    msg4 = make_message("!")

    # Add a single message and make sure it does not trigger a completed group
    assert inst.add_message(msg1) is None

    # Add a second message and make sure it triggers a completed group
    check_series(inst.add_message(msg2), [[msg1], [msg2]])

    # Add a third message that will start a new window and not return a result
    assert inst.add_message(msg3) is None

    # Add a fourth message to complete the second window
    check_series(inst.add_message(msg4), [[msg3], [msg4]])


def test_counting_group_multi_content_type():
    """Test that multiple data group series can be handled simultaneously"""
    inst = make_inst(
        window_size=2,
        grouping_input=[Greeting.full_name, ProducerId.full_name],
    )
    greet1 = make_message("hi", data_id="1")
    pid1 = Message.from_data(ProducerId("hi"), data_id="1")
    greet2 = make_message("world", data_id="2")
    pid2 = Message.from_data(ProducerId("world"), data_id="2")

    # Add both first messages. Neither should close a window
    assert inst.add_message(greet1) is None
    assert inst.get_pending_messages() == [greet1]
    assert inst.add_message(pid1) is None
    assert inst.get_pending_messages() == [greet1, pid1]

    # Add both second messages. The last one should complete the final element
    # and close the window
    # NOTE: messages within elements are sorted by creation time
    assert inst.add_message(greet2) is None
    assert inst.get_pending_messages() == [greet2, greet1, pid1]
    check_series(inst.add_message(pid2), [[greet1, pid1], [greet2, pid2]])
    assert inst.get_pending_messages() == [greet2, pid2]


def test_group_element_edge_cases():
    """Test edge cases of the GroupElement object (coverage!)"""
    assert GroupElement().trigger_message is None


def test_group_store_update_fail():
    """Test that when updating the group store fails, it is retried"""
    inst = make_inst(window_size=2, grouping_input=[Greeting.full_name])
    msg1 = make_message("hi")
    msg2 = make_message("there")

    # Set up a mock that will fail to set the first time and work after that
    # NOTE: The "called" var here needs to be mutable, thus a list
    called = [False]
    orig = inst._group_store.set

    def fail_first(*args, **kwargs):
        if not called[0]:
            called[0] = True
            return False
        return orig(*args, **kwargs)

    set_mock = MagicMock(side_effect=fail_first)
    inst._group_store.set = set_mock

    # Add the first msg and make sure the mock was called twice (once to fail)
    assert inst.add_message(msg1) is None
    assert set_mock.call_count == 2

    # Add the second msg and make sure it actually got through to the store
    check_series(inst.add_message(msg2), [[msg1], [msg2]])
