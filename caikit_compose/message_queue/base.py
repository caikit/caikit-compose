"""
This module holds the base class for a pub/sub topic queue
"""

# Standard
from typing import Callable
import abc

# First Party
from caikit.core.toolkit.factory import FactoryConstructible

# Local
from ..message import Message


class MessageQueueBase(FactoryConstructible):
    """The QueueBase provides the interface instances of pub/sub queues"""

    CALLBACK = Callable[[Message], None]

    @abc.abstractmethod
    def subscribe(
        self,
        topic: str,
        group_id: str,
        handler: CALLBACK,
        *,
        is_data_stream: bool = False,
    ) -> str:
        """Subscribe to messages on the given topic. There are two forms of
        subscriptions:

        1. Standard: Shared topic subscription where messages are distributed
            between all actors in the group
        2. Data Stream: A Data Stream topic is an ephemeral topic created to
            hold linked data packets that all belong to a single streamed data
            object.

        Subscriptions for Data Streams must be configured to send all messages
        on the topic to a single consumer within the group. This can take the
        form of a exclusive topic or a failover topic, depending on the backend.

        Args:
            topic:  str
                The name of the topic to subscribe to
            group_id:  str
                The unique id of the group that this subscription belongs to
            handler:  CALLBACK
                The callback to trigger when a message is received

        Kwargs:
            is_data_stream:  bool
                Whether or not this subscription is to a data stream topic

        Returns:
            subscription_id:  str
                Unique ID for this subscription locally
        """

    @abc.abstractmethod
    def unsubscribe(self, subscription_id: str):
        """Remove a subscription by id

        Args:
            subscription_id:  str
                Unique ID for this subscription locally
        """

    @abc.abstractmethod
    def create_topic(self, topic: str):
        """Set up a publication topic for this message queue.

        Args:
            topic:  str
                The name of the topic to create
        """

    @abc.abstractmethod
    def publish(self, topic: str, message: Message):
        """Publish the given message on the given topic

        Args:
            topic:  str
                The topic to publish on
            message:  Message
                The message to publish
        """
