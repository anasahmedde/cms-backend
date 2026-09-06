# tests/test_device_enrollment.py
"""Unit tests for the enrolment linking rule in device_video_shop_group (no DB).

Regression cover for: enrolling a screen with a group but no location left the
screen with no group at all, while the wizard reported a clean success.
"""
import os

os.environ.setdefault("S3_BUCKET", "ci-smoke-test")

from device_video_shop_group import enrollment_link_plan


class TestEnrollmentLinkPlan:
    """Membership and playlist are two SEPARATE decisions.

    device_assignment.gid / .sid are both nullable, so a screen may join a group
    before it has a location. device_video_shop_group.sid is NOT NULL, so the
    group's playlist can only be linked once a location exists.
    """

    def test_group_only_still_records_membership(self):
        # THE REPORTED BUG: picking a group but no location used to link nothing.
        plan = enrollment_link_plan("North", None)
        assert plan["write_assignment"] is True
        assert plan["link_playlist"] is False
        assert plan["playlist_pending_location"] is True

    def test_group_and_shop_links_everything(self):
        plan = enrollment_link_plan("North", "Shop Karachi")
        assert plan["write_assignment"] is True
        assert plan["link_playlist"] is True
        assert plan["playlist_pending_location"] is False

    def test_shop_only_records_membership_without_a_playlist(self):
        plan = enrollment_link_plan(None, "Shop Karachi")
        assert plan["write_assignment"] is True
        assert plan["link_playlist"] is False
        # No group was chosen, so nothing is pending — this must not nag the user.
        assert plan["playlist_pending_location"] is False

    def test_neither_links_nothing(self):
        plan = enrollment_link_plan(None, None)
        assert plan == {"write_assignment": False, "link_playlist": False,
                        "playlist_pending_location": False}

    def test_blank_strings_count_as_not_chosen(self):
        # The wizard sends `group || null`, but empty strings must not create a
        # lookup for a group named "".
        assert enrollment_link_plan("", "") == {
            "write_assignment": False, "link_playlist": False,
            "playlist_pending_location": False,
        }
        assert enrollment_link_plan("North", "")["playlist_pending_location"] is True

    def test_pending_flag_implies_membership_without_playlist(self):
        # Invariant: the flag is only ever set when we really did record a group
        # and really did not link a playlist.
        for g, s in [("North", None), ("North", ""), (None, None), (None, "S"), ("N", "S")]:
            plan = enrollment_link_plan(g, s)
            if plan["playlist_pending_location"]:
                assert plan["write_assignment"] is True
                assert plan["link_playlist"] is False
