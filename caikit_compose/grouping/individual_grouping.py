"""
Grouping type which simply passes on individual messages
"""

# Standard
from typing import List, Union

# First Party
import alog

# Local
from ..message import Message
from .base import GroupingBase

log = alog.use_channel("GPIND")


class IndividualGrouping(GroupingBase):
    """An IndividualGrouping is a degenerate "grouping" which simply passes
    messages through. It exists so that composition can take advantage of the
    GroupingBase interface.
    """

    name = "INDIVIDUAL"

    def __init__(self, *_, **__):
        """No local state needed for an individual passthrough"""

    @alog.logged_function(log.debug2)
    def add_message(self, message: Message) -> Union[None, Message]:
        """Pass the message through!"""
        return message

    def get_pending_messages(self) -> List[Message]:
        """Individual groupings are never pending"""
        return []
