"""
Factory module for all GroupStore types
"""

# First Party
from caikit.core.toolkit.factory import ImportableFactory

# Local
from .local import LocalGroupStore

## Implementation ##############################################################

GROUP_STORE_FACTORY = ImportableFactory("group-store")
GROUP_STORE_FACTORY.register(LocalGroupStore)
