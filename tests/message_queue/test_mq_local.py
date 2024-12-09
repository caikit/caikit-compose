"""
Tests for the local MQ implementation
"""
# Standard
from unittest.mock import MagicMock

# Third Party
import pytest

# First Party
import aconfig

# Local
from caikit_compose.message_queue.local import LocalMessageQueue
from tests.conftest import drain_local_mq, make_message


@pytest.mark.parametrize("use_threads", [True, False])
def test_local_mq_pub_sub(use_threads):
    """Test basic pub/sub using a LocalMessageQueue"""
    cfg = {} if use_threads else {"threads": 0}
    mq = LocalMessageQueue(aconfig.Config(cfg, override_env_vars=False), "")
    topic1 = "hello"
    topic2 = "world"
    group1 = "me"
    group2 = "you"

    # Initialize the two topics
    mq.create_topic(topic1)
    mq.create_topic(topic2)

    # Publish a message with on subscriptions
    mq.publish(topic1, make_message("echo"))

    # Add three consumers on topic1 (two in the same group) and a single on
    # topic2
    consumer1a = MagicMock()
    consumer1b = MagicMock()
    group1_consumers = [consumer1a, consumer1b]
    consumer2 = MagicMock()
    sub_1a_t1 = mq.subscribe(topic1, group1, consumer1a)
    sub_1b_t1 = mq.subscribe(topic1, group1, consumer1b)
    sub_2_t1 = mq.subscribe(topic1, group2, consumer2)
    sub_1a_t2 = mq.subscribe(topic2, group1, consumer1a)

    def reset():
        consumer1a.reset_mock()
        consumer1b.reset_mock()
        consumer2.reset_mock()

    # Publish on topic1 and make sure one of the group1 consumers got it along
    # with the group2 consumer
    msg1 = make_message("hi there")
    mq.publish(topic1, msg1)
    if use_threads:
        drain_local_mq(mq)
    called_group1 = [consumer for consumer in group1_consumers if consumer.called]
    assert len(called_group1) == 1
    called_group1[0].assert_called_once_with(msg1)
    consumer2.assert_called_once_with(msg1)
    reset()

    # Publish on topic2 and make sure only 1a is called
    msg2 = make_message("heyo")
    mq.publish(topic2, msg2)
    if use_threads:
        drain_local_mq(mq)
    consumer1a.assert_called_once_with(msg2)
    assert not consumer1b.called
    assert not consumer2.called
    reset()

    # Unsubscribe 1a from topic 1 and re-publish on topic 1
    mq.unsubscribe(sub_1a_t1)
    msg3 = make_message("howdy")
    mq.publish(topic1, msg3)
    if use_threads:
        drain_local_mq(mq)
    assert not consumer1a.called
    consumer1b.assert_called_once_with(msg3)

    # Remove the rest of the subscriptions
    mq.unsubscribe(sub_1b_t1)
    mq.unsubscribe(sub_2_t1)
    mq.unsubscribe(sub_1a_t2)


def test_local_mq_handler_error():
    """Make sure the queue is robust to callback errors"""
    mq = LocalMessageQueue(
        aconfig.Config({"save_messages": True}, override_env_vars=False), ""
    )
    consumer1 = MagicMock(side_effect=RuntimeError)
    consumer2 = MagicMock()
    topic = "top"
    mq.create_topic(topic)
    mq.subscribe(topic, "one", consumer1)
    mq.subscribe(topic, "two", consumer2)

    # Publish and make sure both got it, even though consumer1 raised
    msg = make_message("boo!")
    mq.publish(topic, msg)
    drain_local_mq(mq)
    consumer1.assert_called_once_with(msg)
    consumer2.assert_called_once_with(msg)
    assert mq.messages[topic] == [msg]


def test_local_mq_shutdown():
    """Make sure shutdown can close the pool and drain all messages"""
    mq = LocalMessageQueue(aconfig.Config({}), "")
    consumer = MagicMock()
    topic = "top"
    mq.create_topic(topic)
    mq.subscribe(topic, "one", consumer)
    msg = make_message("boo!")
    mq.publish(topic, msg)
    mq.shutdown(wait=True)
    consumer.assert_called_once()
