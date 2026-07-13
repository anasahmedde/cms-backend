# tests/test_bulk_enrollment.py
"""Unit tests for the pure logic in bulk_enrollment_api (no DB)."""
import io

import pytest

from bulk_enrollment_api import _parse_csv, _pending_code, validate_rows


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

    def test_device_id_existing_in_tenant_is_skipped_not_error(self):
        r = rows({"device_name": "A", "shop_name": "S", "device_id": "mine"})
        v = validate_rows(r, 1, 50, {"mine"}, set())
        assert v["summary"]["valid"] is True
        assert v["summary"]["will_skip"] == 1
        assert v["rows"][0]["action"] == "skip"

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
