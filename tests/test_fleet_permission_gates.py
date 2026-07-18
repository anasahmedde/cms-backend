# tests/test_fleet_permission_gates.py
"""Unit tests for the dict-user permission helpers behind the fleet
dashboard mutations (viewer read-only enforcement)."""
import pytest
from fastapi import HTTPException

from tenant_context import require_perm, require_user_tenant

VIEWER = {"role": "viewer", "user_type": "company", "tenant_id": 7,
          "permissions": ["view_dashboard", "view_reports"]}
EDITOR = {"role": "editor", "user_type": "company", "tenant_id": 7,
          "permissions": ["view_dashboard", "upload_videos", "manage_videos",
                          "manage_links", "view_reports", "manage_advertisements"]}
MANAGER = {"role": "manager", "user_type": "company", "tenant_id": 7,
           "permissions": ["view_dashboard", "manage_devices", "manage_groups",
                           "manage_shops", "upload_videos", "manage_videos",
                           "manage_links", "view_reports"]}
PLATFORM = {"role": "super_admin", "user_type": "platform", "tenant_id": None,
            "active_tenant_id": None, "permissions": ["company.full_access"]}


class TestRequirePerm:
    def test_viewer_blocked_from_device_mutations(self):
        with pytest.raises(HTTPException) as e:
            require_perm(VIEWER, "manage_devices")
        assert e.value.status_code == 403

    def test_editor_blocked_from_fleet_but_keeps_links(self):
        with pytest.raises(HTTPException):
            require_perm(EDITOR, "manage_devices")
        with pytest.raises(HTTPException):
            require_perm(EDITOR, "manage_groups")
        require_perm(EDITOR, "manage_links")  # rotation/link saves stay editor-usable

    def test_any_of_semantics(self):
        # membership ops accept any structural permission
        require_perm(MANAGER, "manage_devices", "manage_groups", "manage_shops")
        with pytest.raises(HTTPException):
            require_perm(EDITOR, "manage_devices", "manage_groups", "manage_shops")

    def test_platform_full_access_bypasses(self):
        require_perm(PLATFORM, "manage_devices")

    def test_empty_permissions_blocked(self):
        with pytest.raises(HTTPException):
            require_perm({"permissions": None}, "manage_devices")


class TestRequireUserTenant:
    def test_company_user_resolves(self):
        assert require_user_tenant(MANAGER) == 7

    def test_impersonating_platform_resolves(self):
        assert require_user_tenant({**PLATFORM, "active_tenant_id": 3}) == 3

    def test_no_tenant_is_400_not_tenant_1(self):
        with pytest.raises(HTTPException) as e:
            require_user_tenant(PLATFORM)
        assert e.value.status_code == 400


class TestReportRange:
    def test_defaults_and_bounds(self):
        from fastapi import HTTPException
        import pytest as _pytest
        from reports_api import _range_or_422
        start, end = _range_or_422(None, None)
        assert (end - start).days == 6  # default = last 7 days inclusive
        s, e = _range_or_422("2026-01-01", "2026-01-31")
        assert str(s) == "2026-01-01" and str(e) == "2026-01-31"
        with _pytest.raises(HTTPException):
            _range_or_422("2026-02-01", "2026-01-01")  # inverted
        with _pytest.raises(HTTPException):
            _range_or_422("2020-01-01", "2026-01-01")  # too large
        with _pytest.raises(HTTPException):
            _range_or_422("nope", None)
