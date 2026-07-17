# tests/test_template_logic.py
"""Unit tests for the pure logic in template_api (no DB required)."""
import pytest

from template_api import (
    make_qr_png,
    resolve_zone,
    validate_content_payload,
    validate_zones,
    _canonical_s3_ref,
    _s3_bucket_key,
    _collapse_content,
)


class FakeCursor:
    """Minimal cursor: routes each execute() to canned rows for the base
    (template_zone_content) query vs the group (template_zone_group_content)
    query, so _collapse_content precedence can be tested with no DB."""

    def __init__(self, base_rows, group_rows):
        self._base = base_rows
        self._group = group_rows
        self._pending = []

    def execute(self, sql, params=None):
        self._pending = self._group if "template_zone_group_content" in sql else self._base

    def fetchall(self):
        return self._pending


class TestCollapsePrecedence:
    # Precedence must be screen > group > location > company.
    def test_group_overrides_location_and_company(self):
        base = [("z1", "company", {"t": "C"}), ("z1", "shop", {"t": "S"})]
        group = [("z1", {"t": "G"})]
        out = _collapse_content(FakeCursor(base, group), 1, shop_id=10, device_id=99, group_id=5)
        assert out["z1"] == {"t": "G"}

    def test_screen_overrides_group(self):
        base = [("z1", "shop", {"t": "S"}), ("z1", "device", {"t": "D"})]
        group = [("z1", {"t": "G"})]
        out = _collapse_content(FakeCursor(base, group), 1, shop_id=10, device_id=99, group_id=5)
        assert out["z1"] == {"t": "D"}

    def test_group_applies_regardless_of_shop(self):
        # A device with no shop-scope content still gets the group's content.
        base = [("z1", "company", {"t": "C"})]
        group = [("z1", {"t": "G"})]
        out = _collapse_content(FakeCursor(base, group), 1, shop_id=None, device_id=99, group_id=5)
        assert out["z1"] == {"t": "G"}

    def test_no_group_id_ignores_group_table(self):
        # group_id=None must NOT run the group query at all.
        base = [("z1", "company", {"t": "C"})]
        exploding = [("z1", {"t": "SHOULD_NOT_APPEAR"})]
        out = _collapse_content(FakeCursor(base, exploding), 1, shop_id=None, device_id=99, group_id=None)
        assert out["z1"] == {"t": "C"}

    def test_per_zone_layering(self):
        # Different zones resolve at different levels independently.
        base = [("hdr", "company", {"t": "C"}), ("hdr", "device", {"t": "D"}),
                ("promo", "shop", {"t": "S"})]
        group = [("promo", {"t": "G"}), ("banner", {"t": "GB"})]
        out = _collapse_content(FakeCursor(base, group), 1, shop_id=10, device_id=99, group_id=5)
        assert out["hdr"] == {"t": "D"}      # screen wins
        assert out["promo"] == {"t": "G"}    # group beats shop
        assert out["banner"] == {"t": "GB"}  # group-only zone

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

    def test_text_runs_valid(self):
        zone = {"key": "combo", "type": "text", "x": 0, "y": 0, "w": 100, "h": 30, "z": 1,
                "binding": {"source": "static"},
                "content": {"runs": [
                    {"text": "Big", "x": 5, "y": 5, "font_size_vh": 40, "bold": True, "text_color": "#ffffff"},
                    {"text": "small note", "x": 5, "y": 60, "font_size_vh": 15, "align": "left"},
                ]}}
        assert validate_zones([zone]) == []

    def test_text_runs_bad_position(self):
        zone = {"key": "combo", "type": "text", "x": 0, "y": 0, "w": 100, "h": 30, "z": 1,
                "binding": {"source": "static"},
                "content": {"runs": [{"text": "x", "x": 120, "y": 5}]}}
        assert any("content.runs[0].x must be a number 0-100" in e for e in validate_zones([zone]))

    def test_text_runs_only_on_text_zones(self):
        zone = {"key": "vid", "type": "media", "x": 0, "y": 0, "w": 50, "h": 50, "z": 1,
                "binding": {"source": "content", "scope": "shop"},
                "content": {"runs": [{"text": "x", "x": 5, "y": 5}]}}
        assert any("content.runs is only valid on text/ticker zones" in e for e in validate_zones([zone]))

    def test_resolve_passes_runs_through(self):
        zone = {"key": "combo", "type": "text", "x": 0, "y": 0, "w": 100, "h": 30, "z": 1,
                "binding": {"source": "static"},
                "content": {"text": "fallback", "runs": [{"text": "A", "x": 1, "y": 1, "font_size_vh": 20}]}}
        out = resolve_zone(zone, {}, {}, lambda s: "https://x/" + s)
        assert out["content"]["runs"] == [{"text": "A", "x": 1, "y": 1, "font_size_vh": 20}]

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
        # style.bg_color folds into resolved content so the Android renderer
        # (which draws content-level backgrounds) shows designer colors.
        assert out["content"] == {"text": "Shop Karachi 12", "bg_color": "#0a1628"}
        assert out["style"]["bg_color"] == "#0a1628"
        assert (out["x"], out["y"], out["w"], out["h"]) == (0, 0, 100, 15)

    def test_playlist_marker(self):
        out = resolve_zone(WHITEBOARD_ZONES[1], ENTITY, {}, FAKE_PRESIGN)
        assert out["content"] == {"playlist": True}

    def test_missing_entity_falls_back_to_empty(self):
        out = resolve_zone(WHITEBOARD_ZONES[0], {}, {}, FAKE_PRESIGN)
        assert out["content"] == {"text": "", "bg_color": "#0a1628"}

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


class TestParseBgValue:
    # Color/gradient parsing needs no DB (cur unused for those branches).
    def test_solid_color(self):
        from template_api import parse_bg_value
        assert parse_bg_value(None, 1, "#0a1628") == {"bg_color": "#0a1628"}

    def test_gradient_two_stops_with_angle(self):
        from template_api import parse_bg_value
        out = parse_bg_value(None, 1, "#0a1628 -> #f59e0b @120")
        assert out["bg_gradient"]["stops"] == ["#0a1628", "#f59e0b"]
        assert out["bg_gradient"]["angle"] == 120

    def test_gradient_default_angle(self):
        from template_api import parse_bg_value
        assert parse_bg_value(None, 1, "#000000, #ffffff")["bg_gradient"]["angle"] == 135


class TestResolveMediaValue:
    def test_external_image_url(self):
        from template_api import resolve_media_value
        assert resolve_media_value(None, 1, "https://x.com/a.png") == {"media_url": "https://x.com/a.png", "media_type": "image"}

    def test_external_video_url(self):
        from template_api import resolve_media_value
        assert resolve_media_value(None, 1, "https://x.com/clip.mp4")["media_type"] == "video"

    def test_s3_uri(self):
        from template_api import resolve_media_value
        assert resolve_media_value(None, 1, "s3://b/k.jpg") == {"media_s3": "s3://b/k.jpg", "media_type": "image"}


class TestParseQrValue:
    def test_link_to_encode(self):
        from template_api import parse_qr_value
        assert parse_qr_value(None, 1, "https://menu.example.com/x") == {"qr_mode": "link", "qr_link": "https://menu.example.com/x"}

    def test_image_url_shown_as_is(self):
        from template_api import parse_qr_value
        out = parse_qr_value(None, 1, "https://x.com/qr.png")
        assert out["qr_mode"] == "image" and out["media_url"].endswith("qr.png")


class TestValidateGradientPayload:
    def test_gradient_ok(self):
        assert validate_content_payload("text", {"bg_gradient": {"stops": ["#000", "#fff"], "angle": 90}}) == []

    def test_gradient_bad_stop(self):
        errs = validate_content_payload("text", {"bg_gradient": {"stops": ["blue", "#fff"]}})
        assert any("hex color" in e for e in errs)

    def test_media_url_external_ok(self):
        assert validate_content_payload("media", {"media_url": "https://x.com/a.jpg", "media_type": "image"}) == []

    def test_media_url_must_be_http(self):
        errs = validate_content_payload("media", {"media_url": "ftp://x/a"})
        assert any("http(s) URL" in e for e in errs)


class TestMakeQrPng:
    def test_generates_png_bytes(self):
        png = make_qr_png("https://example.com/menu")
        assert png[:8] == b"\x89PNG\r\n\x1a\n"
        assert len(png) > 200


class TestS3RefNormalization:
    """A media-library item's s3_link can be stored as s3://, an S3 https URL, or
    a bare key. All must normalize to canonical s3://bucket/key so a library image
    both saves and resolves on the player. A genuine non-S3 URL must NOT normalize."""

    def test_s3_uri_unchanged(self):
        assert _canonical_s3_ref("s3://digix-videos/ads/promo.jpg") == "s3://digix-videos/ads/promo.jpg"

    def test_virtual_hosted_https(self):
        assert _canonical_s3_ref(
            "https://digix-videos.s3.us-east-2.amazonaws.com/ads/promo.jpg"
        ) == "s3://digix-videos/ads/promo.jpg"

    def test_path_style_https(self):
        assert _canonical_s3_ref(
            "https://s3.us-east-2.amazonaws.com/digix-videos/ads/promo.jpg"
        ) == "s3://digix-videos/ads/promo.jpg"

    def test_bare_key(self):
        assert _canonical_s3_ref("ads/promo.jpg").endswith("/ads/promo.jpg")
        assert _canonical_s3_ref("ads/promo.jpg").startswith("s3://")

    def test_non_s3_url_left_alone(self):
        # An external CDN URL is not an S3 object — leave it so validation rejects
        # it (it belongs in media_url, not media_s3).
        assert _canonical_s3_ref("https://cdn.example.com/promo.jpg") == "https://cdn.example.com/promo.jpg"
        assert _s3_bucket_key("https://cdn.example.com/promo.jpg") == (None, None)

    def test_normalized_ref_passes_validation(self):
        payload = {"media_s3": _canonical_s3_ref(
            "https://digix-videos.s3.amazonaws.com/ads/promo.jpg"), "media_type": "image"}
        assert validate_content_payload("media", payload) == []

    def test_media_zone_resolves_library_image_to_url(self):
        zone = {"key": "media", "type": "media", "x": 0, "y": 0, "w": 30, "h": 30, "z": 1,
                "binding": {"source": "content"}}
        payload = {"media_s3": "s3://digix-videos/ads/promo.jpg", "media_type": "image"}
        out = resolve_zone(zone, {}, {"media": payload}, lambda u: "https://signed/" + u)
        assert out["content"]["media_url"].startswith("https://signed/")
        assert out["content"]["media_type"] == "image"


class TestVersionedMediaKeys:
    def test_media_key_changes_per_upload(self):
        # Devices cache by URL path — a replaced file must get a NEW path.
        from template_api import _content_media_key
        a = _content_media_key("acme", "device", 7, "promo", "jpg")
        b = _content_media_key("acme", "device", 7, "promo", "jpg")
        assert a != b
        assert a.startswith("tenants/acme/template-content/device/7/promo-")
        assert a.endswith(".jpg")

    def test_qr_key_changes_per_generation(self):
        from template_api import _content_qr_key
        a = _content_qr_key("acme", "company", 1, "menu_qr")
        b = _content_qr_key("acme", "company", 1, "menu_qr")
        assert a != b and a.endswith(".png")
