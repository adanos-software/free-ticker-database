from __future__ import annotations

from scripts.build_quality_contract import PROFILE_LEVEL, check


def selected_failures(checks, profile):
    level = PROFILE_LEVEL[profile]
    return [item for item in checks if PROFILE_LEVEL[item["scope"]] <= level and item["status"] != "pass"]


def test_merge_profile_does_not_hide_merge_failure() -> None:
    checks = [check("merge", False), check("stable", False, scope="stable")]
    assert [item["name"] for item in selected_failures(checks, "merge")] == ["merge"]


def test_stable_includes_merge_and_stable_gates() -> None:
    checks = [check("merge", False), check("stable", False, scope="stable"), check("complete", False, scope="complete")]
    assert [item["name"] for item in selected_failures(checks, "stable")] == ["merge", "stable"]


def test_complete_includes_every_lower_profile() -> None:
    checks = [check("merge", False), check("stable", False, scope="stable"), check("complete", False, scope="complete")]
    assert len(selected_failures(checks, "complete")) == 3
