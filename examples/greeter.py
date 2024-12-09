"""
Minimal example that implements a greeter using caikit-compose.

This example has a single Actor which responds to input with a greeting. The
actor keeps track of users it's already seen and responds accordingly.
"""
# Standard
from functools import partial

# First Party
from caikit.core import DataObjectBase, dataobject
import caikit.core.toolkit.logging

# Local
from caikit_compose import (
    GROUP_STORE_FACTORY,
    MQ_FACTORY,
    ActorBase,
    GroupStoreBase,
    Message,
    MessageQueueBase,
    SubscriptionManager,
)

## Library #####################################################################


@dataobject
class Name(DataObjectBase):
    first_name: str
    last_name: str

    def __str__(self) -> str:
        return f"{self.first_name} {self.last_name}"


@dataobject
class Greeting(DataObjectBase):
    text: str


class GreeterActor(ActorBase):
    def __init__(
        self,
        initial_greeting: str = "Hi {first_name} {last_name}, nice to meet you!",
        return_greeting: str = "Hey {first_name}, welcome back!",
    ):
        self.initial_greeting = initial_greeting
        self.return_greeting = return_greeting
        self._known_users = set()

    def greet(self, name: Name) -> str:
        name_str = str(name)
        template = (
            self.return_greeting
            if name_str in self._known_users
            else self.initial_greeting
        )
        self._known_users.add(name_str)
        return template.format(first_name=name.first_name, last_name=name.last_name)

    def handle_message(self, output_mq: MessageQueueBase, message: Message):
        greeting = self.greet(message.unwrapped)
        output_mq.publish(
            Greeting.full_name,
            Message.from_data(Greeting(greeting), data_id=message.header.data_id),
        )

    def subscribe(
        self,
        mq: MessageQueueBase,
        gs: GroupStoreBase,
        **__,
    ) -> SubscriptionManager:
        mq.create_topic(Name.full_name)
        mq.create_topic(Greeting.full_name)
        return SubscriptionManager(
            f"greeter-{id(self)}",
            message_queue=mq,
            group_store=gs,
            actor_run_callback=partial(self.handle_message, mq),
            grouping_input=Name.full_name,
            grouping_type="INDIVIDUAL",
        )


def report_greeting(message: Message):
    print(f"RESPONSE: {message.unwrapped.text}")


## Main ########################################################################


def main():
    caikit.core.logging.configure()
    mq = MQ_FACTORY.construct({"type": "LOCAL", "config": {"threads": 0}})
    gs = GROUP_STORE_FACTORY.construct({"type": "LOCAL"})
    actor = GreeterActor()
    actor.subscribe(mq, gs)
    SubscriptionManager(
        "reporter", mq, gs, report_greeting, Greeting.full_name, "INDIVIDUAL"
    )
    while True:
        first_name = None
        while not first_name:
            first_name = input("First Name [q]: ")
            if first_name == "q":
                return
        last_name = None
        while not last_name:
            last_name = input("Last Name [q]: ")
            if last_name == "q":
                return
        mq.publish(Name.full_name, Message.from_data(Name(first_name, last_name)))


if __name__ == "__main__":
    main()
