"""
Base class functionality for storing grouping messages
"""

# Standard
from typing import Optional
import abc

# First Party
from caikit.core.data_model.base import DataBase
from caikit.core.toolkit.factory import FactoryConstructible


class GroupStoreBase(FactoryConstructible):
    """This is the base class for all implementations of the GroupStore. The
    GroupStore serves the function of providing shared state between running
    instances of a group's aggregator.
    """

    @abc.abstractmethod
    def get(self, grouping_id: str) -> Optional[DataBase]:
        """Get the current state of the grouping for the given grouping

        Args:
            grouping_id:  str
                The unique id for the grouping

        Returns:
            grouping_state:  Optional[DataBase]
                The current state of the grouping. If the grouping is not found,
                None is returned.
        """

    @abc.abstractmethod
    def set(self, grouping_id: str, grouping_state: DataBase) -> bool:
        """Try to set the state of the grouping. If the state was successfully
        updated, True is returned, otherwise False.

        Args:
            grouping_id:  str
                The unique id for the grouping
            grouping_state:  DataBase
                The updated state of the grouping

        Returns:
            success: bool
                True if the grouping was successfully updated, False otherwise.
                If false is returned, this typically means that a race condition
                was reached with another running instance of the aggregator and
                the current instance should retry to process the updated
                grouping message.
        """
