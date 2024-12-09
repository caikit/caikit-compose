"""
Tests for a caikit model actor
"""
# Standard
from unittest.mock import MagicMock
import time

# Third Party
import pytest

# First Party
from caikit.core import DataObjectBase
from caikit.core.data_model import ProducerId

# Local
from caikit_compose.actors.model_actor import ModelActor
from caikit_compose.message import Message
from tests.conftest import drain_local_mq, make_gs, make_mq
from tests.sample_caikit_lib import (
    LongTextDetection,
    LongTextDetector,
    ProducerAwareLongDetector,
    WhitespaceWordCounter,
    WordCount,
)

## Tests #######################################################################


@pytest.mark.parametrize(
    ["input_text", "output_count", "metadata"],
    [
        ("This is a test. Really!", 5, None),
        ("This is another !", 3, None),
        ("This is another !", 4, {"punct": ".,"}),
    ],
)
def test_model_actor_task_model_unary_unary(input_text, output_count, metadata):
    """Make sure a model that implements a task can correctly use the task's
    unary-unary run signature
    """
    # Create the actor and subscribe it
    mq = make_mq()
    gs = make_gs()
    word_count_actor = ModelActor(WhitespaceWordCounter())
    word_count_actor.subscribe(mq, gs)

    # Add a listener mock for the output
    output_listener = MagicMock()
    mq.subscribe(WordCount.full_name, "output", output_listener)

    # Get a handle to the UnaryUnary input data object type for the WordCountTask
    UUInput = DataObjectBase.get_class_for_name(
        "caikit_data_model.WordCountTaskUnaryUnary"
    )

    # Send a message and make sure it reached the model
    mq.publish(
        UUInput.full_name,
        Message.from_data(UUInput(text=input_text), metadata=metadata),
    )
    drain_local_mq(mq)

    # Make sure the model was called and generated output to the output topic
    output_listener.assert_called_once()
    call_args = output_listener.call_args.args
    assert len(call_args) == 1
    wc_msg = call_args[0]
    assert wc_msg.unwrapped.word_count == output_count


def test_model_actor_no_task():
    """Make sure a model with no task correctly parses the run signature"""
    # Create the actor and subscribe it
    mq = make_mq()
    gs = make_gs()
    word_count_actor = ModelActor(LongTextDetector(long_len=5))
    word_count_actor.subscribe(mq, gs)

    # Add a listener mock for the output
    output_listener = MagicMock()
    mq.subscribe(LongTextDetection.full_name, "output", output_listener)

    # Send a message and make sure it reached the model
    mq.publish(WordCount.full_name, Message.from_data(WordCount(word_count=6)))
    drain_local_mq(mq)

    # Make sure the model was called and generated output to the output topic
    output_listener.assert_called_once()
    call_args = output_listener.call_args.args
    assert len(call_args) == 1
    wc_msg = call_args[0]
    assert wc_msg.unwrapped.length == 6


def test_model_actor_multi_input():
    """Make sure a model with multiple data model inputs correctly sets up a key
    grouping
    """
    # Create the actor and subscribe it
    mq = make_mq()
    gs = make_gs()
    word_count_actor = ModelActor(ProducerAwareLongDetector("Gabe"))
    word_count_actor.subscribe(mq, gs)

    # Add a listener mock for the output
    output_listener = MagicMock()
    mq.subscribe(LongTextDetection.full_name, "output", output_listener)

    # Send both input messages
    data_id = "data"
    mq.publish(
        WordCount.full_name, Message.from_data(WordCount(word_count=6), data_id=data_id)
    )
    mq.publish(
        ProducerId.full_name,
        Message.from_data(ProducerId(name="Gabe"), data_id=data_id),
    )
    drain_local_mq(mq)

    # Make sure the model was called and generated output to the output topic
    output_listener.assert_called_once()
    call_args = output_listener.call_args.args
    assert len(call_args) == 1
    wc_msg = call_args[0]
    assert wc_msg.unwrapped.length == 6
