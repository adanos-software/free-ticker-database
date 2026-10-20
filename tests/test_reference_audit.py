import json

import pytest

from scripts.lib.reference_audit import sha256, snapshot_metadata


def test_snapshot_sidecar_cannot_hide_a_changed_source(tmp_path):
    path = tmp_path / "source.json"
    path.write_text('{"data": 1}')
    path.with_suffix(".json.meta.json").write_text(json.dumps({"sha256": sha256(path), "last_modified": "recorded"}))
    assert snapshot_metadata(path, "https://example.test")["last_modified"] == "recorded"
    path.write_text('{"data": 2}')
    with pytest.raises(ValueError, match="hash disagrees"):
        snapshot_metadata(path, "https://example.test")
