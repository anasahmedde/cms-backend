# tests/test_bulk_enrollment.py
"""Unit tests for the pure logic in bulk_enrollment_api (no DB)."""
import io

import pytest

from bulk_enrollment_api import _parse_csv, _pending_code, validate_rows, _row_changes, content_columns_for, _content_summary


class TestContentSummary:
    def test_fit_only_payload_reads_as_fit(self):
        # A style-only (Fit) change must not read as "set" (which implies media).
        assert _content_summary({"fit_mode": "contain"}) == "fit: contain"

    def test_media_still_wins_over_fit(self):
        assert _content_summary({"media_s3": "s3://b/promo.jpg", "fit_mode": "cover"}) == "promo.jpg"

    def test_empty_is_blank(self):
        assert _content_summary({}) == ""


class TestContentColumns:
    def test_media_zone_gets_media_and_fit_columns(self):
        zones = [{"key": "promo", "type": "media", "binding": {"source": "content"}}]
        cols = content_columns_for(zones)
        headers = [c[0] for c in cols]
        assert headers == ["content.promo.media", "content.promo.fit"]
        # the fit column is tagged so the parser routes it to fit_mode
        assert ("content.promo.fit", "promo", "fit", "media") in cols

    def test_text_zone_unaffected(self):
        zones = [{"key": "hdr", "type": "text", "binding": {"source": "content"}}]
        headers = [c[0] for c in content_columns_for(zones)]
        assert headers == ["content.hdr.text", "content.hdr.bg"]

    def test_non_content_zone_skipped(self):
        zones = [{"key": "main", "type": "media", "binding": {"source": "device.playlist"}}]
        assert content_columns_for(zones) == []


class TestRowChanges:
    CUR = {"name": "Old", "shop": "Shop A", "group": "North",
           "content": {"qr": {"qr_link": "https://old"}}}

    def test_no_changes_when_identical(self):
        assert _row_changes("Old", "Shop A", "North", {"qr": {"qr_link": "https://old"}}, self.CUR) == []

    def test_blank_leaves_as_is(self):
        # A blank group cell must NOT clear the current group.
        assert _row_changes("Old", "Shop A", "", {}, self.CUR) == []

    def test_only_the_changed_field(self):
        ch = _row_changes("New", "Shop A", "North", {}, self.CUR)
        assert ch == [{"field": "name", "from": "Old", "to": "New"}]

    def test_shop_group_and_content_change(self):
        ch = _row_changes("Old", "Shop B", "South", {"qr": {"qr_link": "https://new"}}, self.CUR)
        fields = {c["field"] for c in ch}
        assert fields == {"location", "group", "content.qr"}

    def test_new_device_all_fields_are_changes(self):
        ch = _row_changes("A", "S", "G", {}, {})
        assert {c["field"] for c in ch} == {"name", "location", "group"}


def rows(*recs):
    out = []
    for r in recs:
        d = {"device_name": "", "shop_name": "", "group_name": "", "device_id": "", "resolution": "", "notes": ""}
        d.update(r)
        out.append(d)
    return out


class TestValidateRows:
    def test_happy_mixed(self):
        r = rows(
            {"device_name": "Screen A", "shop_name": "Shop 1", "device_id": "androidid1"},
            {"device_name": "Screen B", "shop_name": "Shop 1", "group_name": "North"},  # pending
        )
        v = validate_rows(r, existing_device_count=0, max_devices=50,
                          mobile_ids_this_tenant=set(), mobile_ids_other_tenant=set())
        assert v["summary"]["valid"] is True
        assert v["summary"]["will_create"] == 1
        assert v["summary"]["will_pending"] == 1
        assert v["summary"]["new_shops"] == ["Shop 1"]
        assert v["summary"]["new_groups"] == ["North"]
        assert [x["action"] for x in v["rows"]] == ["create", "pending"]

    def test_required_fields(self):
        v = validate_rows(rows({"device_id": "x1"}), 0, 50, set(), set())
        reasons = " ".join(e["reason"] for e in v["errors"])
        assert "device_name is required" in reasons
        assert "shop_name is required" in reasons
        assert v["summary"]["valid"] is False

    def test_duplicate_device_id_in_file(self):
        r = rows(
            {"device_name": "A", "shop_name": "S", "device_id": "dup"},
            {"device_name": "B", "shop_name": "S", "device_id": "dup"},
        )
        v = validate_rows(r, 0, 50, set(), set())
        assert any("duplicate device_id" in e["reason"] for e in v["errors"])

    def test_device_id_owned_by_other_tenant_rejected(self):
        r = rows({"device_name": "A", "shop_name": "S", "device_id": "foreign"})
        v = validate_rows(r, 0, 50, set(), {"foreign"})
        assert any("another company" in e["reason"] for e in v["errors"])

    def test_device_id_existing_in_tenant_is_update_not_create(self):
        # An existing screen is valid and never counts as a new create; without a DB
        # baseline (cur=None) its non-blank fields read as an update.
        r = rows({"device_name": "A", "shop_name": "S", "device_id": "mine"})
        v = validate_rows(r, 1, 50, {"mine"}, set())
        assert v["summary"]["valid"] is True
        assert v["summary"]["will_create"] == 0
        assert v["summary"]["will_update"] + v["summary"]["will_unchanged"] == 1
        assert v["rows"][0]["action"] in ("update", "unchanged")

    def test_bad_resolution(self):
        r = rows({"device_name": "A", "shop_name": "S", "resolution": "huge"})
        v = validate_rows(r, 0, 50, set(), set())
        assert any("resolution must look like" in e["reason"] for e in v["errors"])

    def test_quota_exceeded(self):
        r = rows(*[{"device_name": f"D{i}", "shop_name": "S"} for i in range(5)])
        v = validate_rows(r, existing_device_count=48, max_devices=50,
                          mobile_ids_this_tenant=set(), mobile_ids_other_tenant=set())
        assert v["summary"]["quota"]["ok"] is False
        assert any("Quota exceeded" in e["reason"] for e in v["errors"])
        assert v["summary"]["valid"] is False

    def test_quota_zero_means_unlimited(self):
        r = rows(*[{"device_name": f"D{i}", "shop_name": "S"} for i in range(100)])
        v = validate_rows(r, 0, 0, set(), set())
        assert v["summary"]["quota"]["ok"] is True

    def test_placeholder_device_id_rejected(self):
        r = rows({"device_name": "A", "shop_name": "S", "device_id": "pending:ABC123"})
        v = validate_rows(r, 0, 50, set(), set())
        assert any("placeholder" in e["reason"] or "invalid characters" in e["reason"] for e in v["errors"])


class TestParseCsv:
    def test_skips_comments_and_reads_canonical_columns(self):
        csv_text = (
            "# instructions line\n"
            "device_name,shop_name,group_name,device_id,resolution,notes\n"
            "Screen A,Shop 1,North,androidid1,1080x1920,hi\n"
            ",,,,,\n"  # blank row skipped
            "Screen B,Shop 2,,,,\n"
        )
        recs = _parse_csv(csv_text.encode())
        assert len(recs) == 2
        assert recs[0]["device_name"] == "Screen A"
        assert recs[0]["device_id"] == "androidid1"
        assert recs[1]["shop_name"] == "Shop 2"

    def test_header_in_any_column_order(self):
        csv_text = "shop_name,device_name\nShop 9,Screen Z\n"
        recs = _parse_csv(csv_text.encode())
        assert recs[0]["device_name"] == "Screen Z"
        assert recs[0]["shop_name"] == "Shop 9"

    def test_bom_tolerated(self):
        recs = _parse_csv("﻿device_name,shop_name\nA,S\n".encode("utf-8"))
        assert recs[0]["device_name"] == "A"


def test_pending_code_is_unambiguous():
    for _ in range(50):
        c = _pending_code()
        assert len(c) == 6
        assert not (set(c) & set("O0I1"))


class TestFleetExportCells:
    """_cell_of must be the exact inverse of _parse_row_content's grammar, so
    re-uploading an unmodified fleet export diffs to zero."""

    def test_text_and_fit_round_trip(self):
        from bulk_enrollment_api import _cell_of
        assert _cell_of("text", {"text": "50% OFF"}, {}) == "50% OFF"
        assert _cell_of("fit", {"fit_mode": "contain"}, {}) == "contain"
        assert _cell_of("text", {}, {}) == ""

    def test_bg_color_gradient_and_image(self):
        from bulk_enrollment_api import _cell_of
        assert _cell_of("bg", {"bg_color": "#111827"}, {}) == "#111827"
        cell = _cell_of("bg", {"bg_gradient": {"stops": ["#0a1628", "#f59e0b"], "angle": 90}}, {})
        assert cell == "#0a1628 -> #f59e0b @90"
        assert _cell_of("bg", {"bg_image_url": "https://cdn/x.jpg"}, {}) == "https://cdn/x.jpg"

    def test_gradient_cell_reparses_identically(self):
        # The exported gradient string must re-parse to the same payload.
        import re as _re
        from bulk_enrollment_api import _cell_of
        cell = _cell_of("bg", {"bg_gradient": {"stops": ["#0a1628", "#f59e0b"], "angle": 135}}, {})
        hexes = _re.findall(r"#[0-9a-fA-F]{3,8}", cell)
        angle = int(_re.search(r"@(\d{1,3})", cell).group(1))
        assert hexes == ["#0a1628", "#f59e0b"] and angle == 135

    def test_media_prefers_library_name(self):
        from bulk_enrollment_api import _cell_of
        lib = {"s3://bucket/videos/promo.mp4": "Summer Promo"}
        assert _cell_of("media", {"media_s3": "s3://bucket/videos/promo.mp4",
                                  "media_type": "video"}, lib) == "Summer Promo"

    def test_media_type_mismatch_exports_blank(self):
        # An extension-less video URL can't round-trip (the parser would re-derive
        # "image" and downgrade the zone) — the cell must stay blank.
        from bulk_enrollment_api import _cell_of
        assert _cell_of("media", {"media_url": "https://stream.example.com/live",
                                  "media_type": "video"}, {}) == ""
        # …but a matching extension exports fine.
        assert _cell_of("media", {"media_url": "https://cdn/x.mp4",
                                  "media_type": "video"}, {}) == "https://cdn/x.mp4"

    def test_qr_link_wins(self):
        from bulk_enrollment_api import _cell_of
        assert _cell_of("qr", {"qr_mode": "link", "qr_link": "https://menu.example.com",
                               "qr_generated_s3": "s3://b/qr.png"}, {}) == "https://menu.example.com"


class TestPendingRoundTrip:
    def test_existing_pending_id_is_unchanged_not_error(self):
        # A fleet export carries pending:CODE ids; re-uploading them must be a
        # no-op (update path), not "must be a real device id".
        rows = [{"device_name": "Lobby", "shop_name": "Shop A", "group_name": "",
                 "device_id": "pending:ABC123", "resolution": "", "notes": ""}]
        out = validate_rows(rows, existing_device_count=1, max_devices=0,
                            mobile_ids_this_tenant={"pending:ABC123"},
                            mobile_ids_other_tenant=set())
        assert out["summary"]["valid"] is True
        assert out["rows"][0]["action"] in ("unchanged", "update")

    def test_unknown_pending_id_still_rejected(self):
        rows = [{"device_name": "Lobby", "shop_name": "Shop A", "group_name": "",
                 "device_id": "pending:TYPED", "resolution": "", "notes": ""}]
        out = validate_rows(rows, existing_device_count=0, max_devices=0,
                            mobile_ids_this_tenant=set(), mobile_ids_other_tenant=set())
        assert any("placeholder" in e["reason"] for e in out["errors"])

    def test_existing_screen_without_location_round_trips(self):
        # Fleet export writes blank shop_name for screens with no location —
        # blank means "leave as-is" for EXISTING ids, so it must validate clean.
        rows = [{"device_name": "Orphan", "shop_name": "", "group_name": "",
                 "device_id": "abc123", "resolution": "", "notes": ""}]
        out = validate_rows(rows, existing_device_count=1, max_devices=0,
                            mobile_ids_this_tenant={"abc123"}, mobile_ids_other_tenant=set())
        assert out["summary"]["valid"] is True

    def test_new_row_still_requires_shop(self):
        rows = [{"device_name": "New Screen", "shop_name": "", "group_name": "",
                 "device_id": "newdev1", "resolution": "", "notes": ""}]
        out = validate_rows(rows, existing_device_count=0, max_devices=0,
                            mobile_ids_this_tenant=set(), mobile_ids_other_tenant=set())
        assert any("shop_name is required" in e["reason"] for e in out["errors"])


class TestNameGuards:
    """Location/group cells must match existing names exactly; a case/spacing
    near-miss is a typo that would auto-create a junk duplicate — hard error."""

    EXISTING = {"lahore shop": "Lahore Shop", "north region": "North Region"}

    def _row(self, shop, group=""):
        return [{"device_name": "S1", "shop_name": shop, "group_name": group,
                 "device_id": "dev1", "resolution": "", "notes": ""}]

    def _run(self, rows):
        return validate_rows(rows, existing_device_count=0, max_devices=0,
                             mobile_ids_this_tenant=set(), mobile_ids_other_tenant=set(),
                             existing_shops=self.EXISTING, existing_groups=self.EXISTING)

    def test_exact_match_ok(self):
        out = self._run(self._row("Lahore Shop", "North Region"))
        assert out["summary"]["valid"] is True
        assert out["summary"]["new_shops"] == []  # existing, not "new"

    def test_case_mismatch_is_error_with_suggestion(self):
        out = self._run(self._row("lahore shop"))
        assert any("use 'Lahore Shop'" in e["reason"] for e in out["errors"])

    def test_spacing_mismatch_is_error(self):
        out = self._run(self._row("North  Region", "north region"))
        # shop cell 'North  Region' → but EXISTING keys are for shops here too
        assert any("use 'North Region'" in e["reason"] for e in out["errors"])

    def test_genuinely_new_name_allowed_and_listed(self):
        out = self._run(self._row("Shop Karachi 13"))
        assert out["summary"]["valid"] is True
        assert out["summary"]["new_shops"] == ["Shop Karachi 13"]

    def test_no_guard_when_names_not_supplied(self):
        out = validate_rows(self._row("lahore shop"), existing_device_count=0, max_devices=0,
                            mobile_ids_this_tenant=set(), mobile_ids_other_tenant=set())
        assert out["summary"]["valid"] is True


class TestTemplateColumn:
    NAMES = {"portrait menu": "Portrait Menu", "landscape promo": "Landscape Promo"}

    def _row(self, template, dev="dev1"):
        return [{"device_name": "S1", "shop_name": "Shop A", "group_name": "",
                 "template": template, "device_id": dev, "resolution": "", "notes": ""}]

    def _run(self, rows, mine=None):
        return validate_rows(rows, existing_device_count=0, max_devices=0,
                             mobile_ids_this_tenant=mine or set(), mobile_ids_other_tenant=set(),
                             template_names=self.NAMES)

    def test_exact_name_ok_and_diffed(self):
        out = self._run(self._row("Portrait Menu"), mine={"dev1"})
        assert out["summary"]["valid"] is True
        row = out["rows"][0]
        assert row["action"] == "update"
        assert any(c["field"] == "template" and c["to"] == "Portrait Menu" for c in row["changes"])

    def test_near_miss_errors_with_exact_name(self):
        out = self._run(self._row("portrait menu"))
        assert any("use 'Portrait Menu'" in e["reason"] for e in out["errors"])

    def test_unknown_template_is_error_listing_available(self):
        out = self._run(self._row("Does Not Exist"))
        assert any("isn't linked to your company" in e["reason"]
                   and "Landscape Promo" in e["reason"] for e in out["errors"])

    def test_blank_template_is_inherit(self):
        out = self._run(self._row(""))
        assert out["summary"]["valid"] is True

    def test_no_guard_when_templates_not_supplied(self):
        out = validate_rows(self._row("Anything"), existing_device_count=0, max_devices=0,
                            mobile_ids_this_tenant=set(), mobile_ids_other_tenant=set())
        assert out["summary"]["valid"] is True
