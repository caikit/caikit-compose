"""
Tests for the core Message class
"""
# Standard
from datetime import datetime
import uuid

# Third Party
import pytest

# First Party
from caikit.core import DataObjectBase, dataobject

# Local
from caikit_compose.message import PACKAGE, Message, MessageHeader

## Helpers #####################################################################


@dataobject
class Foo(DataObjectBase):
    foo: int
    bar: str


## Tests #######################################################################


def test_message_header_defaults():
    """Make sure defaulting is set up correctly"""
    now = datetime.now()
    hdr = MessageHeader(content_type="bar")
    assert isinstance(hdr.data_id, str)
    assert len(hdr.data_id) == len(str(uuid.uuid4()))
    assert hdr.content_type == "bar"
    assert hdr.creation_time > now
    assert hdr.roi_id == 0


@pytest.mark.parametrize("obj_class", [Message, MessageHeader])
def test_proto_packagers(obj_class):
    """Make sure both objects use the right proto package"""
    proto_class = obj_class.get_proto_class()
    assert obj_class.full_name == proto_class.DESCRIPTOR.full_name
    assert obj_class.full_name == f"{PACKAGE}.{proto_class.DESCRIPTOR.name}"


def test_message_wrapping():
    """Test wrapping and unwrapping an arbitrary data object"""
    foo = Foo(42, "baz")
    md = {"asdf": "qwer"}
    msg = Message.from_data(foo, md, roi_id=123)
    assert msg.header.roi_id == 123
    foo2 = msg.unwrapped
    assert foo.to_dict() == foo2.to_dict()


def test_message_to_from_proto():
    """Make sure Message can round trip with protobuf"""
    foo = Foo(42, "baz")
    msg = Message.from_data(foo)
    msg2 = Message.from_proto(msg.to_proto())
    assert msg2.unwrapped == foo


def test_message_to_from_dict():
    """Make sure Message can round trip with dict"""
    foo = Foo(42, "baz")
    msg = Message.from_data(foo)
    msg2 = Message.from_dict(msg.to_dict())
    assert msg2.unwrapped == foo


def test_message_to_from_json():
    """Make sure Message can round trip with json"""
    foo = Foo(42, "baz")
    msg = Message.from_data(foo)
    msg2 = Message.from_json(msg.to_json())
    assert msg2.unwrapped == foo


@pytest.mark.parametrize(
    ["key", "val"],
    [
        ("nested.foo", 42),
        ("nested.bar", "baz"),
        ("header.data_id", "some-data"),
        ("metadata.foo", "bar"),
    ],
)
def test_message_nested_get(key, val):
    """Make sure nested fields can be extracted from wrapped messages"""

    @dataobject
    class NestedFoo(DataObjectBase):
        nested: Foo

    nfoo = NestedFoo(Foo(42, "baz"))
    msg = Message.from_data(nfoo, data_id="some-data", metadata={"foo": "bar"})
    assert msg.nested_get(key) == val


def test_message_metadata_default_dict():
    """Make sure that the common ways of constructing a Message always have a
    dict for metadata
    """
    assert Message().metadata == {}
    assert Message.from_data(Foo(1)).metadata == {}
