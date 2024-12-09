"""
Minimal example of multiple listeners on the message queue
"""

# Standard
from functools import partial
import operator

# First Party
from caikit.core import DataObjectBase, dataobject

# Local
from caikit_compose import MQ_FACTORY, Message


@dataobject
class Number(DataObjectBase):
    val: float


def operation(mq, topic, c, oper, msg):
    if op := getattr(operator, oper, None):
        mq.publish(
            topic,
            Message.from_data(
                Number(op(msg.unwrapped.val, c)),
                data_id=msg.header.data_id,
                metadata={"oper": oper},
            ),
        )


def report(msg):
    oper = msg.nested_get("metadata.oper")
    print(f"RESULT {oper}: {msg.unwrapped.val}")


if __name__ == "__main__":
    mq = MQ_FACTORY.construct({"type": "LOCAL", "config": {"threads": 0}})
    mq.create_topic("input")
    mq.create_topic("output")
    c_val = float(input("C Val: "))
    for op in ["mul", "truediv", "add", "sub"]:
        mq.subscribe("input", op, partial(operation, mq, "output", c_val, op))
    mq.subscribe("output", "", report)
    while True:
        x = float(input("X: "))
        mq.publish("input", Message.from_data(Number(x)))
