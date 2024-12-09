"""
Tests for the subscription manager
"""
# Standard
from unittest.mock import MagicMock, patch
import threading

# Third Party
import pytest

# First Party
from caikit.interfaces.common.data_model import ProducerId

# Local
from caikit_compose.grouping import GROUPING_FACTORY
from caikit_compose.grouping.individual_grouping import IndividualGrouping
from caikit_compose.grouping.key_grouping import KeyGrouping
from caikit_compose.grouping.subscription_manager import SubscriptionManager
from caikit_compose.message import Message
from tests.conftest import Greeting, drain_local_mq, make_gs, make_message, make_mq

## Tests #######################################################################


def test_individual_subscription():
    """Test a simple individual message subscription"""
    actor_mock = MagicMock()
    mq = make_mq()
    mq.create_topic(Greeting.full_name)
    mgr = SubscriptionManager(
        subscription_id="subid",
        message_queue=mq,
        group_store=make_gs(),
        actor_run_callback=actor_mock,
        grouping_input=Greeting.full_name,
        grouping_type=IndividualGrouping.name,
    )
    assert not actor_mock.called
    assert mgr.id == "subid"
    assert mgr.input_topics == [Greeting.full_name]

    # Send a message and make sure it makes its way to the actor
    msg1 = make_message("hi there")
    mq.publish(Greeting.full_name, msg1)
    drain_local_mq(mq)
    actor_mock.assert_called_once_with(msg1)
    actor_mock.reset_mock()

    # Send another to make sure
    msg2 = make_message("yello!")
    mq.publish(Greeting.full_name, msg2)
    drain_local_mq(mq)
    actor_mock.assert_called_once_with(msg2)


def test_invalid_grouping_type():
    """Make sure a ValueError is raised for an invalid grouping type"""
    with pytest.raises(ValueError):
        SubscriptionManager(
            subscription_id="subid",
            message_queue=make_mq(),
            group_store=make_gs(),
            actor_run_callback=MagicMock(),
            grouping_input=Greeting.full_name,
            grouping_type="FOOBAR",
        )


def test_close_grouping():
    """Make sure that the grouping's close method is called when the
    subscription instance is closed
    """
    mq = make_mq()
    mq.create_topic(Greeting.full_name)
    grouping_factory_mock = MagicMock()
    grouping_mock = grouping_factory_mock()
    stub_message = "MESSAGE"
    close_mock = MagicMock(return_value=stub_message)
    actor_mock = MagicMock()
    grouping_mock.close = close_mock
    with patch.object(GROUPING_FACTORY, "construct", grouping_factory_mock):
        mgr = SubscriptionManager(
            subscription_id="subid",
            message_queue=mq,
            group_store=make_gs(),
            actor_run_callback=actor_mock,
            grouping_input=Greeting.full_name,
            grouping_type=IndividualGrouping.name,
        )
        mgr.close()
        close_mock.assert_called_once()
        actor_mock.assert_called_once_with(stub_message)
        grouping_mock.notify_not_busy.assert_called_once()


def test_group_notify_not_busy():
    """Make sure that the grouping's notify_not_busy gets called when the actor
    finishes working
    """
    mq = make_mq()
    mq.create_topic(Greeting.full_name)
    grouping_factory_mock = MagicMock()
    grouping_mock = grouping_factory_mock()
    stub_message = "MESSAGE"
    add_message = MagicMock(return_value=stub_message)
    actor_mock = MagicMock()
    grouping_mock.add_message = add_message
    with patch.object(GROUPING_FACTORY, "construct", grouping_factory_mock):
        mgr = SubscriptionManager(
            subscription_id="subid",
            message_queue=mq,
            group_store=make_gs(),
            actor_run_callback=actor_mock,
            grouping_input=Greeting.full_name,
            grouping_type=IndividualGrouping.name,
        )
        mgr.handle_message(make_message("stub"))
        add_message.assert_called_once()
        grouping_mock.notify_not_busy.assert_called_once()


def test_processing_messages():
    """Make sure that set of processing messages is maintained correctly"""

    class BlockingActor:
        def __init__(self, end_event: threading.Event):
            self.end_event = end_event
            self._messages = []

        def __call__(self, message):
            message.event.set()
            self.end_event.wait()
            self._messages.append(message)

    class MessageWithEvent:
        def __init__(self, event: threading.Event, wrapped_message: Message):
            self.message = wrapped_message
            self.event = event

        def __getattr__(self, name: str):
            if name == "event":
                return self.event
            return getattr(self.message, name)

    # Set up the actor which won't complete until the event unblocks
    actor_event = threading.Event()
    actor = BlockingActor(actor_event)

    # Set up the manager with the blocking actor
    mq = make_mq(threads=0)
    mq.create_topic(Greeting.full_name)
    mgr = SubscriptionManager(
        subscription_id="subid",
        message_queue=mq,
        group_store=make_gs(),
        actor_run_callback=actor,
        grouping_input=Greeting.full_name,
        grouping_type=IndividualGrouping.name,
    )

    # Send two messages in separate threads and make sure the number of
    # processing messages is kept correctly
    def send_msg(start_event: threading.Event):
        msg = MessageWithEvent(start_event, make_message("hi there"))
        mq.publish(Greeting.full_name, msg)

    start_event1 = threading.Event()
    th1 = threading.Thread(target=send_msg, args=(start_event1,))
    start_event2 = threading.Event()
    th2 = threading.Thread(target=send_msg, args=(start_event2,))
    assert mgr.processing_count == 0
    assert mgr.get_state().processing_messages == []
    th1.start()
    start_event1.wait()
    assert mgr.processing_count == 1
    assert len(mgr.get_state().processing_messages) == 1
    th2.start()
    start_event2.wait()
    assert mgr.processing_count == 2
    assert len(mgr.get_state().processing_messages) == 2
    actor.end_event.set()
    th1.join()
    th2.join()
    assert mgr.processing_count == 0
    assert len(mgr.get_state().processing_messages) == 0


def test_get_state_pending():
    """Make sure that pending messages are correctly reported in get_state"""
    actor_mock = MagicMock()
    mq = make_mq(threads=0)
    mq.create_topic(Greeting.full_name)
    mq.create_topic(ProducerId.full_name)
    sub_id = "subid"
    mgr = SubscriptionManager(
        subscription_id=sub_id,
        message_queue=mq,
        group_store=make_gs(),
        actor_run_callback=actor_mock,
        grouping_input=[Greeting.full_name, ProducerId.full_name],
        grouping_type=KeyGrouping.name,
    )

    # Send the first message and make sure there is one pending message
    greet = make_message("hola", data_id="data")
    mq.publish(Greeting.full_name, greet)
    assert mgr.get_state().pending_messages == [greet]
    actor_mock.assert_not_called()

    # Send the second message and make sure it processes (clearing out pending)
    pid = Message.from_data(ProducerId("foo", "bar"), data_id="data")
    mq.publish(ProducerId.full_name, pid)
    actor_mock.assert_called_once()
    assert not mgr.get_state().pending_messages
