"""
Grouping type that implements grouping messages by a set of keys
"""

# Standard
from typing import List

# First Party
from caikit.core.exceptions import error_handler
import aconfig
import alog

# Local
from ..group_store import GroupStoreBase
from ..message import Message
from .base import GroupingBase
from .compound_grouping_base import CompoundGroup, CompoundGroupingBase

log = alog.use_channel("GPKEY")
error = error_handler.get(log)


class KeyGrouping(CompoundGroupingBase, GroupingBase):
    """A KeyGrouping aggregates multiple messages based on a set of keys
    matching. It will publish a subscription message once all desired content
    types are present for a set of key matches.
    """

    ## Interface ###############################################################

    name = "KEY_GROUPING"

    def __init__(
        self,
        config: aconfig.Config,
        subscription_id: str,
        group_store: GroupStoreBase,
        grouping_input: List[str],
    ):
        """Construct with only the match keys for an element. Since the
        KeyGrouping is the degenerate case of the CompoundGrouping, we only care
        about matching the elements. The "series" is just a single bucket for
        maintaining the in-progress elements.

        #################
        # Config Schema #
        #################

        match_keys:
            description: The set of keys used to match messages
            elements:
                type: str
        """
        super().__init__(
            subscription_id=subscription_id,
            group_store=group_store,
            grouping_input=grouping_input,
            element_match_keys=config.match_keys,
        )

    def series_complete(self, series_msgs: List[Message]) -> bool:
        """A series is complete if it has a message!"""
        return len(series_msgs) == 1

    def make_group_message(self, compound_group: CompoundGroup) -> Message:
        """Grab the single message from the group and return that as the message"""
        error.value_check(
            "<CMP89194972E>",
            len(compound_group.series) == 1,
            "Got unexpected number of messages: {}",
            len(compound_group.series),
        )
        element = compound_group.series[0]
        error.value_check(
            "<CMP83447332E>",
            len(element.messages) == len(self._required_content_types),
            "Got unexpected number of messages in element: {}",
            len(element.messages),
        )
        return Message.from_data(element)

    def trim_window(self, *args, **kwargs) -> List[dict]:
        """We always return a fresh window"""
        return []
