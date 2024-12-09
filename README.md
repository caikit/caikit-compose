# Caikit Compose

The `caikit-compose` project provides a framework for composing multiple `caikit` modules into cohesive realtime AI applications. The project consists of the following core abstractions:

* [Actors](#actors): An `actor` in `caikit-compose` is analogous to a `module` in `caikit`. It performs one or more functions when stimulated by inbound data. Unlike a `module`, an `actor` is responsible for subscribing to its own input data and sending its output data using the [message queue](#message-queue).

* [Message Queue](#message-queue): The Message Queue (often shortened to `mq`) is the central backbone of a `caikit-compose` application. It implements a basic [pub/sub pattern](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) where messages are published to topics which have subscribed listeners. Canonically, those listeners will be [actors](#actors), but can also be any callable as needed by application logic.

* [Grouping](#grouping): The real power of `caikit-compose` comes from its ability to manage groups of messages when presenting them to an [actor](#actors). Some AI functions may require multiple pieces of input data (e.g. raw text plus extracted keywords) and/or multiple sequential pieces of data (e.g. sequence of sentiment scores). With `groupings`, `caikit-compose` implements an extensible framework for managing how messages can be collected to form atomic groups that `actors` can take action on.

## Actors

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/actors)

An `actor` in `caikit-compose` is an atomic unit of action in a realtime AI application. The only required abstract method is `subscribe` which will be called when an `actor` is instantiated to connect it to [message queue](#message-queue) and [group store](#group-store) instances. From there, all actions that the actor takes will be in response to messages received from the `mq` and all outputs that the actor produces will be published to the `mq`.

### Model Actors

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/actors/model_actor.py)

The most common `actor` implementation is a `ModelActor` that wraps an instance of a `caikit module`. This actor class is prebuilt and simply requires an instantiated `model` to be initialized. The `subscribe` method is implemented to inspect the inference method(s) and create subscriptions with the `mq` using the `full_name` of `caikit` data model objects in the inference method signature. When messages arrive on the `mq` for the given topic, the model's inference method (`run` by default) is called and the resulting output is published to the corresponding data object's topic.

## Message Queue

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/message_queue)

In `caikit-compose`, the `Message Queue` abstraction implements a simple best-effort [pub/sub pattern](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern). An instance of an `mq` maintains a set of `topics`, each of which should have a consistent data format (typically a single `dataobject` type). Each `topic` has a set of `subscribers`, and each `subscriber` belongs to a uniquely identified group. Individual [Message](#message) instances are published to a single `topic` and each `subscriber group` on a given `topic` will have a single `subscriber` from the group notified of the published message. This ensures that `subscribers` can be replicated without duplicating the work done for any given message.

The `mq` abstraction is best-effort, which is to say that a message is only ever delivered once. If the `subscriber` that gets notified fails to process fails to handle the message, the message is not retained or retried (there is no `ack`/`nack`). This is done both for simplicity, and to encourage applications that are truly realtime where no individual message is explicitly critical to the overall function of the application. There are no do-overs in real life!

### Message

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/message.py)

The `Message` class is the central currency for the `Message Queue`. It is a special `@dataobject` which is capable of wrapping any arbitrary `@dataobject` for serialization/deserialization and providing attached `header` and `metadata` information. The `header` contains the following fields:

* `data_id`: Application-populated identifier for a unique piece of information. This can be used to provide a relationship between raw input data and derived feature data produced within the application.
* `content_type`: This string is used to uniquely identify the type of the data and should correspond with the expected `content-type` of the `topic` it is published on.
* `creation_time`: This timestamp gets set on construction and can be used to sort message instances chronologically for prioritization and analysis.
* `roi_id`: This ID can be used when the application needs to subdivide a given piece of data into regions of interest.

## Grouping

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/grouping)

Many logical functions in an AI application require multiple pieces of data to be associated with one another. These pieces of data may be created by multiple atomic [actors](#actors) which work independently of one another. In order for a given [actor](#actors) that requires multiple pieces of data to operate, it must be presented with an atomic group of data elements which have been aggregated together.

In `caikit-compose`, this is done using `groupings`. A given `grouping` type manages one strategy for collecting pieces of data into an aggregate group (e.g. match by a header key such as `data_id` or match multiple instances of a given `content_type` into a time sequence). To create a subscription with a `grouping`, the [SubscriptionManager](#subscription-manager) is used. This class acts as an intermediate piece of middleware between the [message queue](#message-queue) and the [actor](#actors) which will collect messages and maintain partial group state in the [group store](#group-store) until the conditions of the `grouping` are met, at which point, it will present the full group to the `actor` as an atomic message.

### Individual Grouping

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/grouping/individual_grouping.py)

The `INDIVIDUAL` group type is the degenerate grouping which performs no grouping at all and simply passes the input messages directly to the `actor`.

### Key Grouping

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/grouping/key_grouping.py)

The `KEY_GROUPING` type is configured to match a collection of `content_type` messages based on the value of one or more fields in the messages. For example, you can aggregate raw text and extracted keywords by using a `KEY_GROUPING` that matches the `header.data_id` field (the default match key).

### Count Grouping

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/grouping/count_grouping.py)

The `COUNT_GROUPING` group extends the logic in `KEY_GROUPING` (allowing for only a single data type as well) to group sequences of messages into sliding windows. The grouping is configured by the collection of data types that should appear in a single frame, the number of frames desired in an atomic sequence, and the stride of the window after a window is sent to the `actor`.

### Custom Groupings

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/grouping/factory.py)

Like other abstractions in the project, `groupings` are managed by a factory which can be extended with new implementations. The factory uses `caikit`'s [ImportableFactory](https://github.com/caikit/caikit/blob/main/caikit/core/toolkit/factory.py#L111) which allows implementations to live in external libraries that are dynamically imported at construction time. Examples of custom grouping semantics might be a grouping which bins input text until a complete sentence is detected and releases individual sentences to the actor.

### Group Store

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/group_store)

The key to the functionality of `groupings` is the ability to store a given group's intermediate state while the `grouping` collects more data. In order to allow this collection to be distributed across multiple instances of a `caikit-compose` process (e.g. multiple `kubernetes` pods), the state of groupings is managed by a simple key/value datastore abstraction called the `GroupStore`. A `LOCAL` implementation is provided using an in-memory dict, but non-local implementations can be added to the `GROUP_STORE_FACTORY` to allow for external state holders (e.g. `redis`) that can be shared across instances.

### Subscription Manager

[Code](https://github.com/caikit/caikit-compose/tree/main/caikit_compose/grouping/subscription_manager.py)

When subscribing an [actor](#actors) to topics on the [message queue](#message-queue), a `SubscriptionManager` is used to bind the `actor`'s inference function(s) to the appropriate groupings. It is the `SubscriptionManager` which takes care of adding messages to the `grouping` and dispatching the result to the actor if (and only if) a group completes based on the input message.

## Examples

You can find all the examples in [examples](https://github.com/caikit/caikit-compose/tree/main/examples/). Here's the simplest "Hello World" to get you started:

```py
from caikit.interfaces.common.data_model import StrSequence
from caikit_compose import MQ_FACTORY, Message

mq = MQ_FACTORY.construct({"type":"LOCAL"})
mq.create_topic("input")

def greet(msg: Message):
    for name in msg.unwrapped.values:
        print(f"Hello {name}!")

mq.subscribe("input", "", greet)

while True:
    x = input("X: ")
    mq.publish("input", Message.from_data(StrSequence(x.split(","))))
```
