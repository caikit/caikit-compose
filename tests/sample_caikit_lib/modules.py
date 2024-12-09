# Standard
from typing import Iterable, Optional
import re
import string

# First Party
from caikit.core import ModuleBase, module
from caikit.core.data_model import DataStream, ProducerId

# Local
from tests.sample_caikit_lib.data_model import (
    LongTextDetection,
    WordCount,
    WordCountTask,
)


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
    def run(self, text: str, punct: Optional[str] = None) -> WordCount:
        return WordCount(len(re.sub(f"[{self._get_punct(punct)}]", " ", text).split()))

    @WordCountTask.taskmethod(input_streaming=True, output_streaming=False)
    def run_stream_unary(
        self,
        text: Iterable[str],
        punct: Optional[str] = None,
    ) -> WordCount:
        all_text = " ".join(text)
        return self.run(all_text, punct)

    @WordCountTask.taskmethod(input_streaming=True, output_streaming=True)
    def run_bidi(
        self,
        text: Iterable[str],
        punct: Optional[str] = None,
    ) -> DataStream[WordCount]:
        total_words = [0]

        def increment_chunk(chunk: str):
            frame = self.run(chunk, punct)
            total_words[0] += frame.word_count
            return WordCount(total_words[0])

        return DataStream.from_iterable([increment_chunk(chunk) for chunk in text])


@module("long-text-detector", "Really silly!", "0.0.0")
class LongTextDetector(ModuleBase):
    def __init__(self, long_len: int = 20):
        self.long_len = long_len

    def run(self, word_count: WordCount) -> Optional[LongTextDetection]:
        if word_count.word_count >= self.long_len:
            return LongTextDetection(length=word_count.word_count, cutoff=self.long_len)


@module("multi-input", "Got to do something", "0.0.0")
class ProducerAwareLongDetector(ModuleBase):
    def __init__(self, long_producer: str):
        self._long_producer = long_producer

    def run(
        self, word_count: WordCount, producer: ProducerId
    ) -> Optional[LongTextDetection]:
        if producer.name == self._long_producer:
            return LongTextDetection(length=word_count.word_count)
