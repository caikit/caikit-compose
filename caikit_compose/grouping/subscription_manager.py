"""
The manager for a single Actor's input subscription
"""

# Standard
from dataclasses import dataclass
from typing import Callable, List, Optional, Union

# First Party
import aconfig
import alog

# Local
from ..group_store import GroupStoreBase
from ..message import Message
from ..message_queue import MessageQueueBase
from .factory import GROUPING_FACTORY

log = alog.use_channel("SBMGR")


@dataclass
class SubscriptionState:
    """Helper struct to represent the current state of the subscription"""

    pending_messages: List[Message]
    processing_messages: List[Message]


class SubscriptionManager:
    """A SubscriptionManager handles the input subscription for a single Actor.
    It takes values for the portions of the Prototype related to the
    subscription and manages the necessary Grouping instance(s) to deliver the
    input messages to the Actor.
    """

    ACTOR_RUN_CALLBACK = Callable[[Message], None]

    def __init__(
        self,
        subscription_id: str,
        message_queue: MessageQueueBase,
        group_store: GroupStoreBase,
        actor_run_callback: ACTOR_RUN_CALLBACK,
        grouping_input: Union[str, List[str]],
        grouping_type: str,
        grouping_config: Optional[aconfig.Config] = None,
        extra_topics: Optional[List[str]] = None,
    ):
        """
        Args:
            subscription_id:  str
                The id for the subscription that created this Actor
            message_queue:  MessageQueueBase
                The shared message queue
            group_store:  GroupStoreBase
                The shared group storage
            actor_run_callback:  ACTOR_RUN_CALLBACK
                The run(message) function for the actor that this manager will
                send messages to when a group message is ready
            grouping_input:  str or List[str]
                The input content type(s) for this subscription
            grouping_type:  str
                The type of grouping that this subscription will manage
            grouping_config:  Optional[aconfig.Config]
                The config object for the given grouping
            extra_topics:  Optional[List[str]]
                Extra message topics to subscribe to. This can be used to add
                side-subscriptions for the grouping that are not part of the
                primary input types (e.g. event listeners)
        """
        self.__subscription_id = subscription_id
        self.__actor_run_callback = actor_run_callback

        # Set up the grouping
        self.__grouping = GROUPING_FACTORY.construct(
            {
                "type": grouping_type,
                "config": grouping_config or aconfig.Config({}),
            },
            subscription_id=subscription_id,
            group_store=group_store,
            grouping_input=grouping_input,
        )

        # Keep track of the number of messages being handled py the actor
        self.__processing_messages = set()

        # Subscribe the grouping to the message queue
        self.input_topics = (
            [grouping_input] if isinstance(grouping_input, str) else grouping_input
        )
        self.extra_topics = extra_topics or []
        for topic in self.topics:
            log.debug2(
                "Registering subscription [%s] group input [%s]: %s",
                subscription_id,
                topic,
                self.__actor_run_callback,
            )
            message_queue.subscribe(
                topic=topic,
                group_id=subscription_id,
                handler=lambda msg: self.handle_message(msg),
            )

    @property
    def topics(self) -> List[str]:
        return self.input_topics + self.extra_topics

    @alog.logged_function(log.debug3)
    def handle_message(self, message: Message):
        """The message handler function that will be subscribed to the queue"""
        log.debug3(
            "Adding message of type %s to subscription %s",
            message.header.content_type,
            self.__subscription_id,
        )
        group_result = self.__grouping.add_message(message)
        if group_result:
            log.debug2("Group ready for [%s]", self.__subscription_id)
            self.__processing_messages.add(group_result)
            self.__actor_run_callback(group_result)
            self.__processing_messages.remove(group_result)
            self.__grouping.notify_not_busy()

    @alog.logged_function(log.debug3)
    def close(self):
        """Close the subscription's grouping"""
        if close_message := self.__grouping.close():
            log.debug2("Closing group ready for [%s]", self.__subscription_id)
            self.__processing_messages.add(close_message)
            self.__actor_run_callback(close_message)
            self.__processing_messages.remove(close_message)
            self.__grouping.notify_not_busy()

    @property
    def processing_count(self) -> int:
        return len(self.__processing_messages)

    @property
    def id(self) -> str:
        return self.__subscription_id

    def get_state(self) -> SubscriptionState:
        return SubscriptionState(
            pending_messages=self.__grouping.get_pending_messages(),
            processing_messages=list(self.__processing_messages),
        )
