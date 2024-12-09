"""
Aggregate a designated number of messages into a sequence ordered by creation
time
"""

# Standard
from datetime import datetime
from typing import Dict, List, Optional, Union
import abc

# First Party
from caikit.core import DataObjectBase, dataobject
from caikit.core.exceptions import error_handler
import alog

# Local
from ..group_store import GroupStoreBase
from ..message import PACKAGE, Message
from ..utils.dict_tools import deep_merge

log = alog.use_channel("GPCMP")
error = error_handler.get(log)


## Data Objects ################################################################


@dataobject(package=PACKAGE)
class GroupElement(DataObjectBase):
    """This object holds a group of messages that constitute a single element of
    a series
    """

    messages: List[Message]

    def __init__(self, messages=None):
        self.messages = messages or []

    def merged_metadata(self) -> dict:
        """Get a merged view of the metadata from the messages"""
        merged_metadata = {}
        for element in self._sorted_by_creation_time():
            merged_metadata = deep_merge(merged_metadata, element.metadata or {})
        return merged_metadata

    @property
    def trigger_message(self) -> Optional[Message]:
        """Get the data_id from the header of the most recent message"""
        if not self.messages:
            return None
        return next(iter(self._sorted_by_creation_time(reverse=True)))

    def _sorted_by_creation_time(self, **kwargs) -> List[Message]:
        return sorted(self.messages, key=lambda m: m.header.creation_time, **kwargs)

    def to_proto(self) -> "GroupElement":
        return self.get_proto_class()(
            messages=[msg.to_proto() for msg in self.messages]
        )


@dataobject(package=PACKAGE)
class CompoundGroupState(DataObjectBase):
    """Object representing the in-flight state of a compound group"""

    partial_items: Dict[str, GroupElement]
    series: List[GroupElement]

    def __init__(
        self,
        partial_items=None,  # Optional[Dict[str, List[Message]]]
        series=None,  # Optional[List[GroupElement]],
    ):
        self.partial_items = partial_items or {}
        self.series = series or []


@dataobject(package=PACKAGE)
class CompoundGroup(DataObjectBase):
    """Object representing a completed grouping to be sent to the Actor"""

    series: List[GroupElement]

    def __init__(self, series):
        sorted_elements = sorted(
            series,
            key=lambda msg: getattr(
                getattr(msg.trigger_message, "header", None),
                "creation_time",
                datetime.min,
            ),
        )
        self.series = list(sorted_elements)

    @property
    def trigger_message(self) -> GroupElement:
        return self.series[-1].trigger_message

    def to_proto(self) -> "GroupElement":
        return self.get_proto_class()(series=[elt.to_proto() for elt in self.series])


## CompoundGroupingBase ########################################################


class CompoundGroupingBase:
    """CompoundGrouping is a base class for groupings which aggregate individual
    messages based on matching key values into atomic elements, then aggregate
    these atomic merged messages into an ordered series based on some windowing
    criteria.
    """

    ## Interface ###############################################################

    # The key to match partial elements on if none given
    _DEFAULT_ELEMENT_MATCH_KEY = "header.data_id"

    def __init__(
        self,
        subscription_id: str,
        group_store: GroupStoreBase,
        grouping_input: List[str],
        element_match_keys: Optional[List[str]] = None,
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
            element_match_keys:  Optional[List[str]]
                The set of keys that will be used to aggregate individual
                messages into atomic element messages within a series
        """
        self.subscription_id = subscription_id
        self.group_id = self._get_group_id()
        self._group_store = group_store
        error.type_check(
            "<CMP15505889E>", list, tuple, set, grouping_input=grouping_input
        )
        error.type_check_all("<CMP96416553E>", str, grouping_input=grouping_input)
        error.value_check(
            "<CMP39207528E>",
            len(grouping_input) > 0,
            "Compound grouping requires at least one grouping_input",
        )
        self._required_content_types = set(grouping_input)

        # Set up the match key lists
        element_match_keys = element_match_keys or [self._DEFAULT_ELEMENT_MATCH_KEY]
        error.type_check("<CMP47580976E>", list, element_match_keys=element_match_keys)
        error.type_check_all(
            "<CMP33362023E>", str, element_match_keys=element_match_keys
        )
        self._element_match_keys = sorted(element_match_keys)

        # Set up the content-type for the output group messages
        self._output_content_type = self.__get_subscription_output_content_type()

    @alog.logged_function(log.debug2)
    def add_message(self, message: Message) -> Union[None, Message]:
        """Process a message that belongs to this subscription. If the new
        message results in a completed group message, it is returned.

        Args:
            message:  Message
                The new message matching one of the group's configured content
                types

        Returns:
            group_message:  Message or None
                If the new message terminates a group message, it is returned,
                otherwise None.
        """
        # Get the id for this element within the group
        element_id = self._get_element_id(message)
        log.debug2(
            "Handling element for %s [%s]: %s", self.name, self.group_id, element_id
        )

        # Get the current state of this group
        group_state: CompoundGroupState = self._group_store.get(self.group_id)
        if group_state is None:
            log.debug2("Initializing group %s", self.group_id)
            group_state = CompoundGroupState()
        log.debug4("State for [%s]: %s", self.group_id, group_state)

        # Determine if the series element is complete
        if (element_msg := group_state.partial_items.get(element_id)) is None:
            log.debug2("Initializing element %s in group %s", element_id, self.group_id)
            element_msg = group_state.partial_items.setdefault(
                element_id, GroupElement()
            )
        element_msg.messages.append(message)

        # If the element is complete, add it to the candidate sequence and
        # remove it from the partial items
        output_msg = None
        if self.__element_message_is_ready(element_msg):
            log.debug3("Element [%s] complete", element_id)
            group_state.series.append(element_msg)
            del group_state.partial_items[element_id]

            # If sequence is ready, create the output message and update the
            # candidate sequence for next time
            if self.series_complete(group_state.series):
                log.debug(
                    "Group [%s] found complete series of size %d",
                    self.group_id,
                    len(group_state.series),
                )
                output_msg = self.make_group_message(CompoundGroup(group_state.series))

                # Update the series of messages for the next iteration
                group_state.series = self.trim_window(group_state.series)

        # Update the state of the group messages, and retry the whole message if
        # the update fails
        # TODO: Better retry/failure semantics for unrecoverable situations
        if not self._group_store.set(self.group_id, group_state):
            log.debug("Failed to update group [%s]. Retrying.", self.group_id)
            return self.add_message(message)

        # Return either None or the produced output message if one was created
        return output_msg

    def get_pending_messages(self) -> List[Message]:
        """Get all messages that are held in the group's state"""
        group_state = self._group_store.get(self.group_id)
        return (
            [
                msg
                for element in group_state.partial_items.values()
                for msg in element.messages
            ]
            + [msg for element in group_state.series for msg in element.messages]
            if group_state
            else []
        )

    ## Abstract Interface ######################################################

    @abc.abstractmethod
    def series_complete(self, series_msgs: List[GroupElement]) -> bool:
        """Child implementations must implement this function to determine when
        a group of messages is ready for publication as a single series.

        Args:
            series_msgs:  List[dict]
                The list of complete element messages that are a candidate for
                publication

        Returns:
            complete:  bool
                True if the series is ready for publication, False otherwise
        """

    @abc.abstractmethod
    def trim_window(self, series_msgs: List[GroupElement]) -> List[GroupElement]:
        """Child implementations must implement this function to update the set
        of in-progress messages that will be retained towards the next
        publication after a group has been published.

        Args:
            series_msgs:  List[GroupElement]
                The list of complete element messages used for the most recent
                publication

        Returns:
            trimmed_series_msgs:  List[GroupElement]
                The list of element messages that should be retained for the
                next publication
        """

    ## Overridable Implementations #############################################

    def make_group_message(self, compound_group: CompoundGroup) -> Message:
        """Wrap the group in a message. The default behavior does this by
        merging metadata.

        Args:
            compound_group:  CompoundGroup
                The group of elements that constitute this group

        Returns:
            group_message:  Message
                The composite group message
        """
        metadata = {}
        for element in compound_group.series:
            metadata = deep_merge(metadata, element.merged_metadata())
        trigger_message = compound_group.trigger_message

        return Message.from_data(
            wrapped_object=compound_group,
            metadata=metadata,
            data_id=trigger_message.header.data_id,
            roi_id=trigger_message.header.roi_id,
            content_type=self._output_content_type,
        )

    ## Protected/Private Implementation Helpers ################################

    def _get_group_id(self) -> str:
        """Unique identifier for this group"""
        return f"{self.name}/{self.subscription_id}"

    def _get_element_id(self, message: Message) -> str:
        """Get a unique id for an element of the series"""
        return ",".join(
            [
                "{}:{}".format(key, message.nested_get(key))
                for key in self._element_match_keys
            ]
        )

    def __get_subscription_output_content_type(self) -> str:
        """Create a unique content type for this grouping's output messages"""
        return f"{self.name}.{self.subscription_id}"

    def __element_message_is_ready(self, element_msg: GroupElement) -> bool:
        """If the individual messages for this element are complete, this will
        create the atomic element message with all of the content types merged.
        """
        elt_msg_types = set([msg.header.content_type for msg in element_msg.messages])
        log.debug2(
            "Considering element parts %s ?= %s",
            elt_msg_types,
            self._required_content_types,
        )
        return elt_msg_types == self._required_content_types
