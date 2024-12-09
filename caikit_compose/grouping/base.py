"""
Base class for a grouping type
"""

# Standard
from typing import List, Optional
import abc

# First Party
from caikit.core.toolkit.factory import FactoryConstructible
import aconfig

# Local
from ..group_store import GroupStoreBase
from ..message import Message


class GroupingBase(FactoryConstructible):
    """Base class for a grouping type"""

    @abc.abstractclassmethod
    def __init__(
        self,
        config: aconfig.Config,
        subscription_id: str,
        group_store: GroupStoreBase,
        grouping_input: List[str],
    ):
        """Construct with the subscription id for the group and the shared group
        store instance.

        Args:
            config:  aconfig.Config
                The config for this grouping type
            subscription_id:  str
                Each grouping belongs to a single subscription
            group_store:  GroupStoreBase
                The shared group store instance that all running aggregators
                will use within the caikit_compose instance.
            grouping_input:  List[str]
                A list of strings representing the content types that should be
                present on a message for it to be sent
        """

    ## Abstract Interface ######################################################

    @abc.abstractmethod
    def add_message(self, message: Message) -> Optional[Message]:
        """Process a message that belongs to this subscription. If the new
        message results in a completed group message, it is returned.

        Args:
            message:  Message
                The new message matching one of the group's configured content
                types

        Returns:
            group_message:  Optional[Message]
                If the new message terminates a group message, it is returned,
                otherwise None.
        """

    @abc.abstractmethod
    def get_pending_messages(self) -> List[Message]:
        """Get a list of messages that are currently pending"""

    ## Overridable Implementations #############################################

    def close(self) -> Optional[Message]:
        """Force a group to close and perform any termination behavior
        appropriate for the group type.
        """

    def notify_not_busy(self):
        """A parent SubscriptionManager will call this when the wrapped actor
        has no active work ongoing. This may be useful for groupings which
        depend on keeping work serialized to their actor.
        """
