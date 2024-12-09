"""
Tests for the individual grouping type
"""

# Local
from caikit_compose.grouping.individual_grouping import IndividualGrouping
from tests.conftest import make_message


def test_individual_grouping():
    """It's so simple it just needs one test!"""
    grp = IndividualGrouping()
    msg = make_message("hi")
    assert grp.add_message(msg) is msg
    assert not grp.get_pending_messages()
