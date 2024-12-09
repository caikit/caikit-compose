"""
The common base representation of a message on a queue that wraps an arbitrary
caikit data object
"""

# Standard
from dataclasses import field
from datetime import datetime
from typing import Any, Optional, Union
import base64
import importlib
import json
import uuid

# Third Party
from google.protobuf import any_pb2

# First Party
from caikit.core.data_model import (
    CAIKIT_DATA_MODEL,
    DataBase,
    DataObjectBase,
    dataobject,
)
from caikit.core.data_model.json_dict import JsonDict, dict_to_struct, struct_to_dict
from caikit.core.exceptions import error_handler
import alog

PACKAGE = f"{CAIKIT_DATA_MODEL}.compose"

log = alog.use_channel("CMSG")
error = error_handler.get(log)


@dataobject(package=PACKAGE)
class MessageHeader(DataObjectBase):
    """The MessageHeader holds metadata about the message that can be used to
    perform filtering and aggregation.
    """

    data_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    content_type: str
    creation_time: datetime = field(default_factory=datetime.now)
    roi_id: int = 0
    # This should never be set by the user so that it is unique!
    uid: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def hash_id(self) -> int:
        return int(uuid.UUID(hex=self.uid))


@dataobject(package=PACKAGE)
class Message(DataObjectBase):
    """The Message class holds a header, metadata, and content"""

    header: MessageHeader
    body: any_pb2.Any
    metadata: JsonDict = field(default_factory=dict)

    _class_delim = "|"
    _no_attr = "__NO_ATTR__"
    _private_slots = ("_unwrapped",)

    def __hash__(self) -> int:
        return self.header.hash_id

    @classmethod
    def from_data(
        cls,
        wrapped_object: DataBase,
        metadata: Optional[JsonDict] = None,
        **header_kwargs,
    ):
        """Initialize with the data object to wrap"""
        body = any_pb2.Any(
            type_url=cls._class_delim.join(
                [wrapped_object.__class__.__module__, wrapped_object.__class__.__name__]
            ),
            value=wrapped_object.to_binary_buffer(),
        )
        header_kwargs.setdefault("content_type", wrapped_object.full_name)
        header = MessageHeader(**header_kwargs)
        inst = cls(header=header, body=body, metadata=metadata or {})
        inst._unwrapped = wrapped_object
        return inst

    @property
    def unwrapped(self) -> DataObjectBase:
        if getattr(self, "_unwrapped", None) is None:
            module_name, class_name = self.body.type_url.split(self._class_delim)
            wrapped_class = getattr(importlib.import_module(module_name), class_name)
            self._unwrapped = wrapped_class.from_binary_buffer(self.body.value)
        return self._unwrapped

    def to_proto(self):
        proto_class = self.get_proto_class()
        return proto_class(
            header=self.header.to_proto(),
            body=self.body,
            metadata=dict_to_struct(self.metadata),
        )

    def to_dict(self):
        return {
            "header": self.header.to_dict(),
            "body": {
                "type_url": self.body.type_url,
                "value": self.body.value,
            },
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, dict_val: dict) -> "Message":
        header = MessageHeader(**dict_val.get("header", {}))
        body = dict_val.get("body")
        if body:
            kwargs = body
            val = body.get("value")
            if isinstance(val, str):
                kwargs["value"] = base64.b64decode(val)
            body = any_pb2.Any(**kwargs)
        return cls(header=header, body=body, metadata=dict_val.get("metadata"))

    @classmethod
    def from_json(cls, json_val: Union[str, dict]) -> "Message":
        """Custom from_json to handle Any"""
        if isinstance(json_val, str):
            json_val = json.loads(json_val)
        return cls.from_dict(json_val)

    @classmethod
    def from_proto(cls, proto) -> "Message":
        """Custom from_proto to handle Any"""
        header = MessageHeader.from_proto(proto.header)
        return cls(
            header=header, body=proto.body, metadata=struct_to_dict(proto.metadata)
        )

    def nested_get(self, key: str) -> Any:
        """Safely get a '.' delimited nested key from the wrapped object

        Args:
            key:  str
                The '.' delimited key (e.g. "foo.bar.baz")

        Returns:
            Whatever the path resolves to, or None if not present
        """
        error.value_check("<CMP57475179E>", key, "Must specify a non-empty key")
        key_parts = key.split(".")
        attr_val = getattr(self, key_parts[0], self._no_attr)
        if attr_val is not self._no_attr:
            current = attr_val
            key_parts = key_parts[1:]
        else:
            current = self.unwrapped
        for i, part in enumerate(key_parts):
            dflt = None if i == len(key_parts) - 1 else {}
            if isinstance(current, dict):
                current = current.get(part, dflt)
            else:
                current = getattr(current, part, dflt)
        return current
