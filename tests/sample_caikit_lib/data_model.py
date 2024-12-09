"""
Data model for sample_caikit_lib
"""

# Standard
from typing import Iterable, List

# First Party
from caikit.core import DataObjectBase, TaskBase, dataobject, task


@dataobject
class WordCount(DataObjectBase):
    word_count: int


@task(
    unary_parameters={"text": str},
    streaming_parameters={"text": List[str]},
    unary_output_type=WordCount,
    streaming_output_type=Iterable[WordCount],
)
class WordCountTask(TaskBase):
    """"""


@dataobject
class LongTextDetection(DataObjectBase):
    length: int
    cutoff: int
