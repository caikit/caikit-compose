"""
Tests for the local group store
"""
# Standard
import threading

# Third Party
import pytest

# First Party
import aconfig

# Local
from caikit_compose.group_store.local import LocalGroupStore
from tests.conftest import make_message


def test_local_gs_get_set():
    """Test basic get/set capabilities"""
    gs = LocalGroupStore(aconfig.Config({}), "")
    gid = "my_group"

    # Nothing to do if not set
    assert gs.get(gid) is None

    # Set a state and retrieve it
    state1 = make_message("hi")
    gs.set(gid, state1)
    assert gs.get(gid) == state1

    # Update a state and retrieve it
    state2 = make_message("there")
    gs.set(gid, state2)
    assert gs.get(gid) == state2

    # Ensure a second group works independently
    gid2 = "other_group"
    assert gs.get(gid2) is None
    gs.set(gid2, make_message("other"))
    assert gs.get(gid2)


def test_local_gs_invalid_type():
    """Make sure that a TypeError occurs if the state is not a data object"""
    gs = LocalGroupStore(aconfig.Config({}), "")
    with pytest.raises(TypeError):
        gs.set("foo", "bar")


def test_local_gs_thread_safety():
    """Make sure that multiple threads correctly block one another between get/
    set logic.
    """
    gs = LocalGroupStore(aconfig.Config({}), "")
    group_id = "my-group"
    started_first = threading.Event()
    finished_second = threading.Event()

    def doit_first():
        state = gs.get(group_id)
        assert not state
        started_first.set()
        finished_second.wait()
        assert not gs.set(group_id, make_message("first"))

    def doit_second():
        started_first.wait()
        state = gs.get(group_id)
        assert not state
        assert gs.set(group_id, make_message("second"))
        finished_second.set()

    th1 = threading.Thread(target=doit_first)
    th2 = threading.Thread(target=doit_second)
    th1.start()
    th2.start()
    th1.join()
    th2.join()

    # At this point, both threads should have had their turn and should have
    # released all locks, so this parent thread should be able to get and check
    state = gs.get(group_id)
    assert state and state.unwrapped.greeting == "second"


def test_local_gs_set_without_get():
    """Make sure that freeing the thread lock does not error when set is called
    without a previous get
    """
    gs = LocalGroupStore(aconfig.Config({}), "")
    group_id = "some_group"
    gs.set(group_id, make_message("first"))

    def th_fn():
        state = gs.get(group_id)
        assert state and state.unwrapped.greeting == "first"
        gs.set(group_id, make_message("second"))

    th = threading.Thread(target=th_fn)
    th.start()
    th.join()

    state = gs.get(group_id)
    assert state and state.unwrapped.greeting == "second"
