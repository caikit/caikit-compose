"""
Local implementation of the message queue interface
"""

# Standard
from concurrent.futures import ThreadPoolExecutor
import uuid

# First Party
import aconfig
import alog

# Local
from ..message import Message
from .base import MessageQueueBase

log = alog.use_channel("MQLOC")


class LocalMessageQueue(MessageQueueBase):
    """Implementation of the MessageQueueBase interface backed by a local
    in-memory dict
    """

    name = "LOCAL"

    def __init__(self, config: aconfig.Config, instance_name: str):
        """Set up the manager and optionally save messages"""
        self._topics = {}
        self._valid_topics = set()
        self._group_indices = {}
        self._save_messages = config.save_messages
        self._pool = (
            ThreadPoolExecutor(max_workers=config.threads)
            if config.threads != 0
            else None
        )
        self.messages = {}
        self.instance_name = instance_name

    def subscribe(
        self,
        topic: str,
        group_id: str,
        handler: MessageQueueBase.CALLBACK,
        *,
        is_data_stream: bool = False,
    ) -> str:
        """Create a subscription using the local dict"""
        sub_id = str(uuid.uuid4())
        self._topics.setdefault(topic, {}).setdefault(group_id, []).append(
            (sub_id, handler, is_data_stream)
        )
        self._group_indices.setdefault(topic, {}).setdefault(group_id, 0)
        return sub_id

    def unsubscribe(self, subscription_id: str):
        """Remove subscription by id"""
        for topic, groups in self._topics.items():
            remove_groups = []
            for group_id, group in groups.items():
                remove_idxs = []
                for i, (sub_id, _, _) in enumerate(group):
                    if sub_id == subscription_id:
                        remove_idxs.append(i)
                for idx in sorted(remove_idxs, reverse=True):
                    del group[idx]
                if not group:
                    remove_groups.append(group_id)
                else:
                    topic_group_idxs = self._group_indices[topic]
                    topic_group_idxs[group_id] = min(
                        topic_group_idxs[group_id], len(group) - 1
                    )
            for group_id in remove_groups:
                del groups[group_id]
                del self._group_indices[topic][group_id]

    def create_topic(self, topic: str):
        """Register the topic"""
        self._valid_topics.add(topic)

    def publish(self, topic: str, message: Message):
        """Publish a message to the given topic"""
        log.debug("Publishing on [%s]: %s", topic, message.header.data_id)
        log.debug4("%s", message)
        assert topic in self._valid_topics, f"Cannot publish to unknown topic [{topic}]"

        # If configured to do so, save the message
        if self._save_messages:
            self.messages.setdefault(topic, []).append(message)

        # Publish once to each subscription group on this topic
        for group_id, group in self._topics.get(topic, {}).items():

            # Choose which member of the group should be called
            group_idx = self._group_indices[topic][group_id]
            log.debug2("Sending message to [%s/%d]", group_id, group_idx)
            sub_id, handler, is_data_stream = group[group_idx]

            # If this is not a data stream, increment group to call the next
            # handler on the next call
            if not is_data_stream:
                group_idx += 1
                if group_idx >= len(group):
                    log.debug3("Cycling group [%s]", group_id)
                    group_idx = 0
                log.debug3("Next index for [%s]: %s", group_id, group_idx)
                self._group_indices[topic][group_id] = group_idx

            # Call the handler
            log.debug2("Calling handler for [%s]", sub_id)
            if self._pool:
                self._pool.submit(handler, message)
            else:
                handler(message)

    ## Utilities ##

    def shutdown(self, wait=True):
        if self._pool:
            self._pool.shutdown(wait=wait)
