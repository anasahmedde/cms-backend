# tests/test_content_approval.py
"""Unit tests for the template-content role gate + approval diversion
(template_api). No DB — fake cursors route the two lookup queries."""
import pytest
from fastapi import HTTPException

from tenant_context import TenantContext
from template_api import (
    _approval_required,
    _canonicalize_payload_s3,
    _require_content_editor,
    _submit_content_approval,
)


def ctx_of(role, perms, user_type="company", tenant_id=7):
    return TenantContext(
        user_id=42, username="u", full_name="U", user_type=user_type,
        tenant_id=tenant_id, active_tenant_id=tenant_id,
        role_name=role, role_id=None, permissions=list(perms),
        company_slug="acme", company_name="Acme",
    )


VIEWER = ctx_of("viewer", ["view_dashboard", "view_reports"])
EDITOR = ctx_of("editor", ["view_dashboard", "upload_videos", "manage_videos",
                           "manage_links", "view_reports", "manage_advertisements"])
MANAGER = ctx_of("manager", ["view_dashboard", "manage_devices", "manage_groups",
                             "manage_shops", "upload_videos", "manage_videos",
                             "manage_links", "view_reports"])
PLATFORM = ctx_of("super_admin", ["company.full_access"], user_type="platform")


class TestRequireContentEditor:
    def test_viewer_is_rejected(self):
        with pytest.raises(HTTPException) as e:
            _require_content_editor(VIEWER)
        assert e.value.status_code == 403

    def test_editor_passes(self):
        _require_content_editor(EDITOR)

    def test_manager_passes(self):
        _require_content_editor(MANAGER)

    def test_platform_full_access_passes(self):
        _require_content_editor(PLATFORM)


class GateCursor:
    """Routes the company-gate query and the user-flag query to canned rows."""

    def __init__(self, company_row, user_row=(False,)):
        self._company = company_row
        self._user = user_row
        self._current = None

    def execute(self, sql, params=None):
        self._current = self._company if "FROM public.company" in sql else self._user

    def fetchone(self):
        return self._current


class TestApprovalRequired:
    def test_platform_user_never_diverted(self):
        assert _approval_required(GateCursor((True, [])), PLATFORM) is False

    def test_gate_off_applies_directly(self):
        assert _approval_required(GateCursor((False, ["admin", "manager"])), EDITOR) is False

    def test_missing_company_row_applies_directly(self):
        assert _approval_required(GateCursor(None), EDITOR) is False

    def test_auto_approve_role_applies_directly(self):
        assert _approval_required(GateCursor((True, ["admin", "manager"])), MANAGER) is False

    def test_editor_is_diverted_when_gate_on(self):
        assert _approval_required(GateCursor((True, ["admin", "manager"])), EDITOR) is True

    def test_auto_roles_default_when_null(self):
        # NULL auto_approve_roles falls back to admin+manager.
        assert _approval_required(GateCursor((True, None)), MANAGER) is False
        assert _approval_required(GateCursor((True, None)), EDITOR) is True

    def test_auto_roles_as_json_string(self):
        assert _approval_required(GateCursor((True, '["editor"]')), EDITOR) is False

    def test_can_approve_content_flag_applies_directly(self):
        assert _approval_required(GateCursor((True, ["admin"]), user_row=(True,)), EDITOR) is False


class SubmitCursor:
    """Captures the supersede UPDATE and the INSERT of the pending request."""

    def __init__(self, superseded_ids=()):
        self.executed = []
        self._superseded = [(i,) for i in superseded_ids]
        self._last = None

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))
        self._last = "update" if sql.lstrip().upper().startswith("UPDATE") else "insert"

    def fetchall(self):
        return self._superseded

    def fetchone(self):
        return (901,)


class NoAuditConn:
    def cursor(self):  # log_audit swallows this — audit must never break the flow
        raise RuntimeError("no audit in unit tests")


class TestSubmitContentApproval:
    def test_pending_row_shape_and_supersede(self):
        cur = SubmitCursor(superseded_ids=[880])
        out = _submit_content_approval(
            NoAuditConn(), cur, EDITOR, action="put", scope="device",
            zone_key="promo", zone_label="Promo box", payload={"text": "hi"},
            device_id=31, target_name="Lobby TV")
        assert out == {"status": "pending_approval", "request_id": 901,
                       "zone_key": "promo", "superseded_request_ids": [880]}
        update_sql, update_params = cur.executed[0]
        insert_sql, insert_params = cur.executed[1]
        # Older pending request from the same user for the same zone+target is cancelled.
        assert "SET status = 'cancelled'" in update_sql
        assert update_params == (7, 42, "device", 31, "promo")
        # The queued request carries everything execute needs to replay the write.
        assert "INSERT INTO public.content_change_request" in insert_sql
        import json
        change_data = json.loads(insert_params[4])
        assert change_data == {"action": "put", "scope": "device", "zone_key": "promo",
                               "zone_label": "Promo box", "payload": {"text": "hi"},
                               "shop_id": None, "device_id": 31, "group_id": None,
                               "requested_by": 42}
        assert insert_params[1] == "device" and insert_params[2] == 31
        assert insert_params[3] == "Lobby TV"

    def test_company_scope_targets_tenant(self):
        cur = SubmitCursor()
        _submit_content_approval(
            NoAuditConn(), cur, EDITOR, action="clear_overrides", scope="company",
            zone_key="qr", zone_label="qr", payload=None)
        _, insert_params = cur.executed[1]
        assert insert_params[1] == "company" and insert_params[2] == 7
        assert insert_params[3] == "Acme"  # falls back to the company name


class TestCanonicalizePayloadS3:
    def test_https_and_bare_keys_become_s3(self):
        p = _canonicalize_payload_s3({"media_s3": "tenant/x.mp4", "text": "hi"})
        assert p["media_s3"].startswith("s3://")

    def test_s3_and_empty_pass_through(self):
        p = _canonicalize_payload_s3({"media_s3": "s3://b/k.mp4", "bg_image_s3": None})
        assert p["media_s3"] == "s3://b/k.mp4" and p["bg_image_s3"] is None
