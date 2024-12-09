"""
Factory module for all message queue types
"""

# First Party
from caikit.core.toolkit.factory import ImportableFactory

# Local
from .local import LocalMessageQueue

## Implementation ##############################################################

MQ_FACTORY = ImportableFactory("message-queue")
MQ_FACTORY.register(LocalMessageQueue)
