"""
Actor implementation that wraps a caikit model
"""

# Standard
from dataclasses import dataclass
from functools import partial
from typing import Annotated, List, Optional, Union, get_args, get_origin
import uuid

# First Party
from caikit.core import ModuleBase
from caikit.core.data_model import DataBase
from caikit.core.data_model.dataobject import make_dataobject
from caikit.core.exceptions import error_handler
from caikit.core.signature_parsing.module_signature import CaikitMethodSignature
import aconfig
import alog

# Local
from .. import GroupStoreBase, Message, MessageQueueBase, SubscriptionManager
from .base import ActorBase

log = alog.use_channel("MAGNT")
error = error_handler.get(log)


@dataclass
class ActorMethod:
    task_name: str
    input_streaming: bool
    output_streaming: bool
    signature: CaikitMethodSignature
    task_parameters: dict[str, type]
    output_type: type


class ModelActor(ActorBase):
    """An Actor that wraps a caikit model instance"""

    model: ModuleBase
    actor_methods: List[ActorMethod]

    def __init__(
        self,
        model: ModuleBase,
        actor_methods: Optional[List[str]] = None,
    ):
        self.model = model
        module_class = self.model.__class__

        # Gather the list of method names
        actor_method_names = actor_methods
        if actor_method_names is None:
            if module_class.tasks:
                log.debug2("Subscribing to all task methods")
                actor_method_names = [
                    entry[2].method_name
                    for lst in module_class._TASK_INFERENCE_SIGNATURES.values()
                    for entry in lst
                ]
            else:
                log.debug2("Falling back to 'run' for subscription method")
                actor_method_names = ["run"]
        error.type_check(
            "<CMP69349982E>", list, tuple, set, actor_methods=actor_method_names
        )
        error.type_check_all("<CMP14958907E>", str, actor_methods=actor_method_names)
        log.debug(
            "Using actor methods %s for model of type %s",
            actor_methods,
            module_class.MODULE_CLASS,
        )

        # Extract the actor method signatures and input/output streaming values
        self.actor_methods = []
        task_method_name_mapping = {
            entry[2].method_name: (task, entry)
            for task, lst in module_class._TASK_INFERENCE_SIGNATURES.items()
            for entry in lst
        }
        for actor_method_name in actor_method_names:

            # Check to see if this is available in a task method
            if task_method_entry := task_method_name_mapping.get(actor_method_name):
                task, (input_streaming, output_streaming, signature) = task_method_entry
                self.actor_methods.append(
                    ActorMethod(
                        task_name=task.__name__,
                        input_streaming=input_streaming,
                        output_streaming=output_streaming,
                        signature=signature,
                        task_parameters=task.get_required_parameters(input_streaming),
                        output_type=self._strip_type_wrapping(
                            task.get_output_type(output_streaming)
                        ),
                    )
                )

            # Otherwise, infer the signature information (assume unary-unary)
            else:
                error.value_check(
                    "<CMP88763332E>",
                    hasattr(module_class, actor_method_name),
                    "Invalid actor method: %s",
                    actor_method_name,
                )
                signature = CaikitMethodSignature(module_class, actor_method_name)
                self.actor_methods.append(
                    ActorMethod(
                        task_name=module_class.__name__,
                        input_streaming=False,
                        output_streaming=False,
                        signature=signature,
                        task_parameters={
                            name: arg_type
                            for name, arg_type in signature.parameters.items()
                            if name not in signature.default_parameters
                        },
                        output_type=self._strip_type_wrapping(signature.return_type),
                    )
                )

    def subscribe(
        self,
        mq: MessageQueueBase,
        gs: GroupStoreBase,
        **grouping_config,
    ) -> List[SubscriptionManager]:
        """Subscribe this actor to the"""
        subscriptions = []
        for actor_method in self.actor_methods:

            # If all required arguments are data objects, wire up direct input
            # types using those types
            if all(
                isinstance(val, type) and issubclass(val, DataBase)
                for val in actor_method.task_parameters.values()
            ):
                grouping_input = [
                    type_.full_name for type_ in actor_method.task_parameters.values()
                ]

                # Use an individual grouping
                if len(actor_method.task_parameters) == 1:
                    log.debug2(
                        "Using input message type for single input: %s",
                        actor_method.task_parameters,
                    )
                    grouping_type = "INDIVIDUAL"

                # Use a key grouping
                else:
                    log.debug2(
                        "Using a key grouping for %s", actor_method.task_parameters
                    )
                    grouping_type = "KEY_GROUPING"

            # Otherwise, use an input message that aggregates all required args
            else:
                input_message_name = "{}{}{}".format(
                    actor_method.task_name,
                    "Stream" if actor_method.input_streaming else "Unary",
                    "Stream" if actor_method.output_streaming else "Unary",
                )
                log.debug2(
                    "Using task-based input message name: %s", input_message_name
                )

                try:
                    input_message = DataBase.get_class_for_name(input_message_name)
                except ValueError:
                    input_message = make_dataobject(
                        name=input_message_name,
                        annotations=actor_method.task_parameters,
                    )
                grouping_input = [input_message.full_name]
                grouping_type = "INDIVIDUAL"

            # Make sure the topics exists
            for input_message_name in grouping_input:
                mq.create_topic(input_message_name)
            if not actor_method.output_streaming:
                mq.create_topic(actor_method.output_type.full_name)
            else:
                # TODO
                pass

            # Define the function that will bind the subscription to the model
            def actor_func(
                message: Message,
                _mq,
                _actor_method,
            ):
                unwrapped = message.unwrapped

                # If all required kwargs are data objects, set them directly in
                # the kwargs
                if all(
                    isinstance(val, type) and issubclass(val, DataBase)
                    for val in _actor_method.task_parameters.values()
                ):
                    if len(_actor_method.task_parameters) == 1:
                        kwargs = {list(_actor_method.task_parameters)[0]: unwrapped}
                    else:
                        # NOTE: This assumes that the _type_ of the arg uniquely
                        # identifies the key! That may be a bad assumption for
                        # some circumstances, but it's guaranteed to be true if
                        # a key grouping is used like it is above.
                        type_names_to_messages = {
                            elt_msg.unwrapped.full_name: elt_msg.unwrapped
                            for elt_msg in unwrapped.messages
                        }
                        kwargs = {
                            kwarg_name: type_names_to_messages.get(kwarg_type.full_name)
                            for kwarg_name, kwarg_type in _actor_method.task_parameters.items()
                        }
                        error.value_check(
                            "<CMP86507469E>",
                            None not in kwargs.values(),
                            "Could not map grouping types to kwargs",
                        )

                # Otherwise, it's a wrapper message that put all the kwargs into
                # field names
                else:
                    kwargs = {
                        name: getattr(unwrapped, name)
                        for name in _actor_method.task_parameters
                    }

                # Fill in optional params from metadata
                kwargs.update(
                    {
                        md_key: md_val
                        for md_key, md_val in message.metadata.items()
                        if md_key in _actor_method.signature.parameters
                        and md_key not in kwargs
                    }
                )
                run_func = getattr(self.model, _actor_method.signature.method_name)
                log.debug4(
                    "Calling run_func [%s] with kwargs: %s",
                    _actor_method.signature.method_name,
                    kwargs,
                )
                res = run_func(**kwargs)
                if res:
                    msg = Message.from_data(
                        res,
                        metadata=message.metadata,
                        data_id=message.header.data_id,
                        roi_id=message.header.roi_id,
                    )
                    log.debug(
                        "Publishing %s model output on topic %s: %s",
                        self.model,
                        res.full_name,
                        msg.header.data_id,
                    )
                    _mq.publish(res.full_name, msg)

            subscriptions.append(
                SubscriptionManager(
                    subscription_id=f"{self.model.__class__.__name__}-{uuid.uuid4()}",
                    message_queue=mq,
                    group_store=gs,
                    actor_run_callback=partial(
                        actor_func,
                        _mq=mq,
                        _actor_method=actor_method,
                    ),
                    grouping_input=grouping_input,
                    grouping_type=grouping_type,  # TODO: Streaming
                    grouping_config=aconfig.Config(
                        grouping_config, override_env_vars=False
                    ),
                )
            )
        return subscriptions

    @staticmethod
    def _strip_type_wrapping(type_: type) -> type:
        if get_origin(type_) in [Union, Annotated]:
            type_args = [
                arg
                for arg in get_args(type_)
                if isinstance(arg, type) and arg is not type(None)
            ]
            error.value_check(
                "<CMP00438977E>",
                len(type_args) == 1,
                "Could not deduce unique type from %s",
                type_,
            )
            return type_args[0]
        return type_
