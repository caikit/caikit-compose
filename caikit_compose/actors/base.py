"""
Define the base abstraction for an Actor
"""

# Standard
from typing import List, Optional, Union
import abc

# Local
from ..group_store import GroupStoreBase
from ..grouping import SubscriptionManager
from ..message_queue import MessageQueueBase


class ActorBase(abc.ABC):
    """An Actor is a class instance which binds behavior to a subscription.
    Subclasses of Actor may bind conventions for how to determine the
    subscription input/output types and the actor function callback.
    """

    @abc.abstractmethod
    def subscribe(
        self,
        mq: MessageQueueBase,
        gs: GroupStoreBase,
        **grouping_config,
    ) -> Optional[Union[SubscriptionManager, List[SubscriptionManager]]]:
        """Subscribe the given actor on the given queue with the given group
        store
        """
