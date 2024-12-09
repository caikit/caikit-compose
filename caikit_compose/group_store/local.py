"""
In-memory implementation of the GroupStore
"""
# Standard
from typing import Optional
import threading

# First Party
from caikit.core.data_model.base import DataBase
from caikit.core.exceptions import error_handler
import aconfig
import alog

# Local
from .base import GroupStoreBase

log = alog.use_channel("GSLOC")
error = error_handler.get(log)


class LocalGroupStore(GroupStoreBase):
    """Implementation of GroupStore that uses an in-memory dict.

    WARNING!
    This version is only suitable for single-instance applications and testing.
    It cannot share state between processes, and it implements thread safety by
    locking the database for each thread between get/set calls.
    """

    name = "LOCAL"

    def __init__(self, config: aconfig.Config, instance_name: str):
        """Set up the internal state dict"""
        self._state = {}
        self._last_get_threads = {}
        self._group_locks = {}
        self.instance_name = instance_name

    def get(self, grouping_id: str) -> Optional[DataBase]:
        """Get the grouping's state"""
        this_thread = threading.get_ident()
        log.debug2("Fetching group [%s] in thread %s", grouping_id, this_thread)
        with self._group_locks.setdefault(grouping_id, threading.Lock()):
            self._last_get_threads[grouping_id] = this_thread
        return self._state.get(grouping_id)

    def set(self, grouping_id: str, grouping_state: DataBase) -> bool:
        """Set the grouping's state. Always succeeds."""
        this_thread = threading.get_ident()
        with self._group_locks.setdefault(grouping_id, threading.Lock()):
            last_get_thread = self._last_get_threads.get(grouping_id)
            if last_get_thread is not None and last_get_thread != this_thread:
                log.debug2(
                    "Failed to set group %s from thread %s since %s fetched last",
                    grouping_id,
                    this_thread,
                    last_get_thread,
                )
                return False
        error.type_check(
            "<CMP86941330E>",
            DataBase,
            grouping_state=grouping_state,
        )
        log.debug2("Updating group [%s]", grouping_id)
        self._state[grouping_id] = grouping_state
        return True
