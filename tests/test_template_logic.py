# tests/test_template_logic.py
"""Unit tests for the pure logic in template_api (no DB required)."""
import pytest

from template_api import (
    make_qr_png,
    resolve_zone,
    validate_content_payload,
    validate_zones,
)

# The whiteboard template: 15% header (shop name), 70% media, bottom 15%
# split into QR / promo pic / promo text.
WHITEBOARD_ZONES = [
    {"key": "header", "type": "text", "x": 0, "y": 0, "w": 100, "h": 15, "z": 1,
     "style": {"bg_color": "#0a1628", "text_color": "#ffffff", "align": "center", "direction": "rtl"},
     "binding": {"source": "shop.name"}},
    {"key": "main", "type": "playlist", "x": 0, "y": 15, "w": 100, "h": 70, "z": 1,
     "binding": {"source": "device.playlist"}},
    {"key": "qr", "type": "qr", "x": 0, "y": 85, "w": 30, "h": 15, "z": 1,
     "style": {"bg_color": "#ffffff"},
     "binding": {"source": "content", "scope": "shop"}},
    {"key": "promo", "type": "media", "x": 30, "y": 85, "w": 25, "h": 15, "z": 1,
     "binding": {"source": "content", "scope": "shop"}},
    {"key": "promo_text", "type": "text", "x": 55, "y": 85, "w": 45, "h": 15, "z": 1,
     "style": {"bg_color": "#111827", "text_color": "#ffffff"},
     "binding": {"source": "content", "scope": "shop"}},
]


class TestValidateZones:
    def test_whiteboard_template_is_valid(self):
        assert validate_zones(WHITEBOARD_ZONES) == []

    def test_not_a_list(self):
        assert validate_zones({"key": "x"}) == ["zones must be a list"]

    def test_duplicate_keys(self):
        zones = [dict(WHITEBOARD_ZONES[0]), dict(WHITEBOARD_ZONES[0])]
        assert any("duplicate key" in e for e in validate_zones(zones))

    def test_bad_zone_key(self):
        zone = dict(WHITEBOARD_ZONES[0], key="Bad Key!")
        assert any("'key' must be a slug" in e for e in validate_zones([zone]))

    def test_unknown_type(self):
        zone = dict(WHITEBOARD_ZONES[0], type="hologram")
        assert any("'type' must be one of" in e for e in validate_zones([zone]))

    def test_zone_out_of_canvas(self):
        zone = dict(WHITEBOARD_ZONES[0], x=50, w=60)
        assert any("extends past the canvas" in e for e in validate_zones([zone]))

    def test_negative_position(self):
        zone = dict(WHITEBOARD_ZONES[0], x=-5)
        assert any("x/y must be within 0-100" in e for e in validate_zones([zone]))

    def test_zero_size(self):
        zone = dict(WHITEBOARD_ZONES[0], w=0)
        assert any("w/h must be within" in e for e in validate_zones([zone]))

    def test_missing_dimension(self):
        zone = {k: v for k, v in WHITEBOARD_ZONES[0].items() if k != "h"}
        assert any("'h' must be a number" in e for e in validate_zones([zone]))

    def test_bad_color(self):
        zone = dict(WHITEBOARD_ZONES[0], style={"bg_color": "blue"})
        assert any("must be a hex color" in e for e in validate_zones([zone]))

    def test_playlist_zone_requires_playlist_binding(self):
        zone = dict(WHITEBOARD_ZONES[1], binding={"source": "static"})
        assert any("must bind to 'device.playlist'" in e for e in validate_zones([zone]))

    def test_only_playlist_zone_may_bind_playlist(self):
        zone = dict(WHITEBOARD_ZONES[0], binding={"source": "device.playlist"})
        assert any("only playlist zones" in e for e in validate_zones([zone]))

    def test_qr_zone_must_bind_content(self):
        zone = dict(WHITEBOARD_ZONES[2], binding={"source": "static"})
        assert any("qr zones must bind to 'content'" in e for e in validate_zones([zone]))

    def test_unknown_binding_source(self):
        zone = dict(WHITEBOARD_ZONES[0], binding={"source": "weather"})
        assert any("binding.source must be one of" in e for e in validate_zones([zone]))

    def test_bad_content_scope(self):
        zone = dict(WHITEBOARD_ZONES[2], binding={"source": "content", "scope": "galaxy"})
        assert any("binding.scope must be one of" in e for e in validate_zones([zone]))

    def test_boolean_rejected_as_number(self):
        zone = dict(WHITEBOARD_ZONES[0], x=True)
        assert any("'x' must be a number" in e for e in validate_zones([zone]))


class TestValidateContentPayload:
    def test_text_payload_ok(self):
        assert validate_content_payload("text", {"text": "Hello", "bg_color": "#fff"}) == []

    def test_unknown_key_rejected(self):
        errs = validate_content_payload("text", {"text": "x", "evil": 1})
        assert any("unknown payload keys" in e for e in errs)

    def test_text_too_long(self):
        errs = validate_content_payload("text", {"text": "x" * 5001})
        assert any("at most 5000" in e for e in errs)

    def test_qr_link_mode_requires_link(self):
        errs = validate_content_payload("qr", {"qr_mode": "link"})
        assert any("requires qr_link" in e for e in errs)

    def test_qr_link_must_be_http(self):
        errs = validate_content_payload("qr", {"qr_mode": "link", "qr_link": "ftp://x"})
        assert any("http(s) URL" in e for e in errs)

    def test_qr_valid_link(self):
        assert validate_content_payload("qr", {"qr_mode": "link", "qr_link": "https://example.com/menu"}) == []

    def test_bad_qr_mode(self):
        errs = validate_content_payload("qr", {"qr_mode": "hologram"})
        assert any("qr_mode must be one of" in e for e in errs)

    def test_payload_must_be_object(self):
        assert validate_content_payload("text", ["nope"]) == ["payload must be an object"]

    def test_bad_color(self):
        errs = validate_content_payload("ticker", {"bg_color": "red"})
        assert any("hex color" in e for e in errs)


FAKE_PRESIGN = lambda uri: f"https://signed.example/{uri.split('/')[-1]}" if uri else None
ENTITY = {"company.name": "MoltyFoam", "shop.name": "Shop Karachi 12", "device.name": "Screen A"}


class TestResolveZone:
    def test_shop_name_binding(self):
        out = resolve_zone(WHITEBOARD_ZONES[0], ENTITY, {}, FAKE_PRESIGN)
        assert out["content"] == {"text": "Shop Karachi 12"}
        assert out["style"]["bg_color"] == "#0a1628"
        assert (out["x"], out["y"], out["w"], out["h"]) == (0, 0, 100, 15)

    def test_playlist_marker(self):
        out = resolve_zone(WHITEBOARD_ZONES[1], ENTITY, {}, FAKE_PRESIGN)
        assert out["content"] == {"playlist": True}

    def test_missing_entity_falls_back_to_empty(self):
        out = resolve_zone(WHITEBOARD_ZONES[0], {}, {}, FAKE_PRESIGN)
        assert out["content"] == {"text": ""}

    def test_qr_link_mode_uses_generated_png(self):
        content = {"qr": {"qr_mode": "link", "qr_link": "https://example.com",
                          "qr_generated_s3": "s3://bucket/qr.png"}}
        out = resolve_zone(WHITEBOARD_ZONES[2], ENTITY, content, FAKE_PRESIGN)
        assert out["content"]["qr_mode"] == "link"
        assert out["content"]["media_url"] == "https://signed.example/qr.png"
        assert out["content"]["media_type"] == "image"

    def test_qr_image_mode_uses_uploaded_media(self):
        content = {"qr": {"qr_mode": "image", "media_s3": "s3://bucket/uploaded-qr.png",
                          "media_type": "image"}}
        out = resolve_zone(WHITEBOARD_ZONES[2], ENTITY, content, FAKE_PRESIGN)
        assert out["content"]["media_url"] == "https://signed.example/uploaded-qr.png"

    def test_qr_media_mode_allows_video(self):
        content = {"qr": {"qr_mode": "media", "media_s3": "s3://bucket/clip.mp4",
                          "media_type": "video"}}
        out = resolve_zone(WHITEBOARD_ZONES[2], ENTITY, content, FAKE_PRESIGN)
        assert out["content"]["media_type"] == "video"

    def test_media_zone_with_no_content_is_empty(self):
        out = resolve_zone(WHITEBOARD_ZONES[3], ENTITY, {}, FAKE_PRESIGN)
        assert out["content"] == {}

    def test_text_zone_content_with_colors(self):
        content = {"promo_text": {"text": "50% OFF", "bg_color": "#ff0000"}}
        out = resolve_zone(WHITEBOARD_ZONES[4], ENTITY, content, FAKE_PRESIGN)
        assert out["content"]["text"] == "50% OFF"
        assert out["content"]["bg_color"] == "#ff0000"

    def test_static_zone(self):
        zone = {"key": "s", "type": "text", "x": 0, "y": 0, "w": 10, "h": 10,
                "binding": {"source": "static"}, "content": {"text": "Fixed"}}
        out = resolve_zone(zone, ENTITY, {}, FAKE_PRESIGN)
        assert out["content"] == {"text": "Fixed"}


class TestMakeQrPng:
    def test_generates_png_bytes(self):
        png = make_qr_png("https://example.com/menu")
        assert png[:8] == b"\x89PNG\r\n\x1a\n"
        assert len(png) > 200
