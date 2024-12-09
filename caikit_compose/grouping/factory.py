"""
Factory for constructing grouping instances
"""
# Standard
from typing import List, Union

# First Party
from caikit.core.exceptions import error_handler
from caikit.core.toolkit.factory import ImportableFactory
import aconfig
import alog

# Local
from ..group_store.base import GroupStoreBase
from .count_grouping import CountGrouping
from .individual_grouping import IndividualGrouping
from .key_grouping import KeyGrouping

log = alog.use_channel("GRPFY")
error = error_handler.get(log)

## Implementation ##############################################################


class GroupingFactory(ImportableFactory):
    """Derived factory that adds subscription_id and group_store to the init
    interface
    """

    def construct(
        self,
        instance_config: Union[dict, aconfig.Config],
        subscription_id: str,
        group_store: GroupStoreBase,
        grouping_input: Union[List[str], str],
    ):
        """Construct an instance of a grouping for the given subscription with
        the shared group store

        Args:
            instance_config:  Union[dict, aconfig.Config]
                The config for this grouping instance
            subscription_id:  str
                The unique ID for the subscription that uses this grouping
            group_store:  GroupStoreBase
                The shared group store instance
            grouping_input:  Union[List[str], str]
                A list of strings representing the content types that should be
                present on a message for it to be sent
        """
        inst_type = instance_config.get(ImportableFactory.TYPE_KEY)
        inst_cls = self._registered_types.get(inst_type)
        error.value_check(
            "<CMP38033745E>",
            inst_cls is not None,
            "No {} class registered for {}",
            self.name,
            inst_type,
        )
        inst_cfg = instance_config.get(ImportableFactory.CONFIG_KEY, {})
        if not isinstance(inst_cfg, aconfig.AttributeAccessDict):
            error.type_check("<CMP49779169E>", dict, instance_config=inst_cfg)
            inst_cfg = aconfig.Config(inst_cfg, override_env_vars=False)
        grouping_input = (
            [grouping_input] if isinstance(grouping_input, str) else grouping_input
        )
        error.type_check(
            "<CMP09157773E>", list, tuple, set, grouping_input=grouping_input
        )
        error.type_check_all("<CMP69595894E>", str, grouping_input=grouping_input)
        return inst_cls(inst_cfg, subscription_id, group_store, grouping_input)


GROUPING_FACTORY = GroupingFactory("grouping")
GROUPING_FACTORY.register(IndividualGrouping)
GROUPING_FACTORY.register(KeyGrouping)
GROUPING_FACTORY.register(CountGrouping)
