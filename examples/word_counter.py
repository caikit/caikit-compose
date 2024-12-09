"""
This example shows how caikit and caikit-compose can be used to implement a
dynamic application which reacts as data flows through the message queue.

In this example, there are two "AI Models" (with extremely basic
implementations). The first counts words in an input text sentence, while the
second takes the counted words and compares it to a threshold to detect "long
text."

The output of both modules is aggregated by a reporting Actor which prints out
the results. The overall topology looks like this:

                       [InputText]      [WordCount]      [LongTextDetection]
                           |                 |                   |
[input loop]-------------->|-v--v--v         |                   |
                           | |  |  |         |                   |
[word counter]<------------|-<  |  |         |                   |
        |----------------------------------->|-v--v              |
                           |    |  |         | |  |              |
[long text detector]<------|----|--|---------|-<  |              |
        |------------------------------------------------------->|-v
                           |    |  |         |    |              | |
[reporter (wc)]<-----------|----<  |         |    |              | |
               ^-----------|-------|---------|----<              | |
                           |       |         |                   | |
[reporter (lt)]<-----------|-------<         |                   | |
               ^-----------|-----------------|-------------------|-<
"""

# Standard
from typing import Iterable, List, Optional, Union
import re
import string

# First Party
from caikit.core import DataObjectBase, ModuleBase, TaskBase, dataobject, module, task
from caikit.core.data_model import DataStream
import alog
import caikit.core.toolkit.logging

# Local
from caikit_compose import (
    GROUP_STORE_FACTORY,
    MQ_FACTORY,
    ActorBase,
    GroupStoreBase,
    Message,
    MessageQueueBase,
    SubscriptionManager,
)
from caikit_compose.actors.model_actor import ModelActor

log = alog.use_channel("EXMPL")


@dataobject
class TextInput(DataObjectBase):
    text: str


@dataobject
class WordCount(DataObjectBase):
    word_count: int


@task(
    unary_parameters={"text": TextInput},
    streaming_parameters={"text": List[TextInput]},
    unary_output_type=WordCount,
    streaming_output_type=Iterable[WordCount],
)
class WordCountTask(TaskBase):
    """"""


@module(
    "whitespace-word-counter",
    "Whitespace word counter",
    "0.0.0",
    tasks=[WordCountTask],
)
class WhitespaceWordCounter(ModuleBase):
    """"""

    def __init__(self, default_punct=string.punctuation):
        self._default_punct = default_punct

    def _get_punct(self, punct):
        if punct is None:
            return self._default_punct
        return punct

    @WordCountTask.taskmethod(input_streaming=False, output_streaming=False)
    def run(
        self, text: Union[str, TextInput], punct: Optional[str] = None
    ) -> WordCount:
        if isinstance(text, TextInput):
            text = text.text
        return WordCount(len(re.sub(f"[{self._get_punct(punct)}]", " ", text).split()))

    @WordCountTask.taskmethod(input_streaming=True, output_streaming=False)
    def run_stream_unary(
        self,
        text: Iterable[TextInput],
        punct: Optional[str] = None,
    ) -> WordCount:
        all_text = " ".join([frame.text for frame in text])
        return self.run(all_text, punct)

    @WordCountTask.taskmethod(input_streaming=True, output_streaming=True)
    def run_bidi(
        self,
        text: Iterable[TextInput],
        punct: Optional[str] = None,
    ) -> DataStream[WordCount]:
        total_words = [0]

        def increment_chunk(chunk: str):
            frame = self.run(chunk, punct)
            total_words[0] += frame.word_count
            return WordCount(total_words[0])

        return DataStream.from_iterable([increment_chunk(chunk) for chunk in text])


@dataobject
class LongTextDetection(DataObjectBase):
    length: int
    cutoff: int


@module("long-text-detector", "Really silly!", "0.0.0")
class LongTextDetector(ModuleBase):
    def __init__(self, long_len: int = 20):
        self.long_len = long_len

    def run(self, word_count: WordCount) -> Optional[LongTextDetection]:
        if word_count.word_count >= self.long_len:
            return LongTextDetection(length=word_count.word_count, cutoff=self.long_len)


class ReporterActor(ActorBase):
    @staticmethod
    def report_word_count(message: Message):
        msgs = message.unwrapped.messages
        log.debug3("Got %s messages for report_word_count", len(msgs))
        log.debug4(msgs)
        text_msg = [msg for msg in msgs if isinstance(msg.unwrapped, TextInput)]
        wc_msg = [msg for msg in msgs if isinstance(msg.unwrapped, WordCount)]
        assert len(text_msg) == 1
        assert len(wc_msg) == 1
        text = text_msg[0].unwrapped
        word_count = wc_msg[0].unwrapped
        print(f"WORD COUNT [{text.text}]: {word_count.word_count}")

    @staticmethod
    def report_long_text(message: Message):
        msgs = message.unwrapped.messages
        log.debug3("Got %s messages for report_long_text", len(msgs))
        log.debug4(msgs)
        text_msg = [msg for msg in msgs if isinstance(msg.unwrapped, TextInput)]
        long_det_msg = [
            msg for msg in msgs if isinstance(msg.unwrapped, LongTextDetection)
        ]
        assert len(text_msg) == 1
        assert len(long_det_msg) == 1
        text = text_msg[0].unwrapped
        long_text = long_det_msg[0].unwrapped
        print(f"FOUND LONG TEXT ({long_text.length}/{long_text.cutoff}): {text.text}")

    @classmethod
    def subscribe(cls, mq: MessageQueueBase, gs: GroupStoreBase, **__):
        wc_sub = SubscriptionManager(
            "word-count-sub",
            message_queue=mq,
            group_store=gs,
            actor_run_callback=cls.report_word_count,
            grouping_type="KEY_GROUPING",
            grouping_input=[TextInput.full_name, WordCount.full_name],
        )
        det_sub = SubscriptionManager(
            "long-text-detect-sub",
            message_queue=mq,
            group_store=gs,
            actor_run_callback=cls.report_long_text,
            grouping_type="KEY_GROUPING",
            grouping_input=[TextInput.full_name, LongTextDetection.full_name],
        )
        return [wc_sub, det_sub]


## Main ########################################################################

if __name__ == "__main__":
    caikit.core.logging.configure()

    mq = MQ_FACTORY.construct({"type": "LOCAL"})
    gs = GROUP_STORE_FACTORY.construct({"type": "LOCAL"})

    # Set up some models and subscriptions
    word_count_actor = ModelActor(WhitespaceWordCounter())
    word_count_actor.subscribe(mq, gs)
    long_detector_actor = ModelActor(LongTextDetector())
    long_detector_actor.subscribe(mq, gs)

    # Set up the reporter actor
    ReporterActor.subscribe(mq, gs)

    # Loop through some inputs
    while True:
        prompt = input("Input [q]: ")
        if prompt.strip() == "q":
            break

        if prompt.strip():
            msg = Message.from_data(TextInput(text=prompt), metadata={"punct": ".,"})
            mq.publish(TextInput.full_name, msg)
