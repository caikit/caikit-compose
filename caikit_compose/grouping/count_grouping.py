"""
Aggregate a designated number of messages into a sequence ordered by creation
time
"""

# Standard
from typing import List

# First Party
from caikit.core.exceptions import error_handler
import aconfig
import alog

# Local
from ..group_store import GroupStoreBase
from .base import GroupingBase
from .compound_grouping_base import CompoundGroupingBase, GroupElement

log = alog.use_channel("GPCNT")
error = error_handler.get(log)

## CountGrouping ###############################################################


class CountGrouping(CompoundGroupingBase, GroupingBase):
    """A CountGrouping aggregates a collection of messages into an ordered
    series of messages of a given size.
    """

    ## Interface ###############################################################

    name = "COUNT_GROUPING"

    def __init__(
        self,
        config: aconfig.Config,
        subscription_id: str,
        group_store: GroupStoreBase,
        grouping_input: List[str],
    ):
        """
        Args:
            subscription_id:  str
                Each grouping belongs to a single subscription. This uniquely
                identifies that subscription within the workspace.
            group_store:  GroupStoreBase
                The shared group store instance that all running aggregators
                will use within the indigo instance.
            grouping_input:  List[str]
                A list of strings representing the content types that should be
                present on a message for it to be sent

        #################
        # Config Schema #
        #################

        window_size:
            description: The size of the window to slide through the received
                messages
            type: number
            required: true

        window_stride:
            description: The number of messages to progress with every returned
                window group
            type: number
            default: 1

        element_match_keys:
            description: The set of keys used to match individual input messages
                when aggregating sequence elements
            elements:
                type: str
        """
        # Initialize the base compound grouping
        super().__init__(
            subscription_id=subscription_id,
            group_store=group_store,
            grouping_input=grouping_input,
            element_match_keys=config.element_match_keys,
        )
        window_size = config.window_size
        error.type_check("<CMP32868391E>", int, window_size=window_size)
        error.value_check("<CMP91580025E>", window_size > 0)
        self._window_size = window_size
        window_stride = config.get("window_stride", 1)
        error.type_check("<CMP32868392E>", int, window_stride=window_stride)
        error.value_check("<CMP91580026E>", window_stride > 0)
        self._window_stride = window_stride

    def series_complete(self, series_msgs: List[GroupElement]) -> bool:
        """A series is complete if the window size has been met"""
        return len(series_msgs) >= self._window_size

    def trim_window(self, series_msgs: List[GroupElement]) -> List[GroupElement]:
        """Slide the window based on the configured stride"""
        return series_msgs[self._window_stride :]
