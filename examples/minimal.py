# First Party
from caikit.interfaces.common.data_model import StrSequence

# Local
from caikit_compose import MQ_FACTORY, Message

mq = MQ_FACTORY.construct({"type": "LOCAL"})
mq.create_topic("input")


def greet(msg: Message):
    for name in msg.unwrapped.values:
        print(f"Hello {name}!")


mq.subscribe("input", "", greet)
while True:
    x = input("X: ")
    mq.publish("input", Message.from_data(StrSequence(x.split(","))))
