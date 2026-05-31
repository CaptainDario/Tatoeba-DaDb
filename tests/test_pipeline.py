"""End-to-end pipeline tests on a Tatoeba subset; synthetic cases in test_regression_groups.py."""

from __future__ import annotations

import json
import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import tatoeba_to_dadb
from tests.deduplication_test_cases import DEDUPLICATION_TEST_CASES
from tests.group_id_test_cases import TEST_CASES as GROUP_ID_CASES
from tests.main_lang_test_cases import MAIN_LANG_TEST_CASES

TMP_DIR = os.path.join("tests", "data")


@pytest.fixture(scope="session", autouse=True)
def check_data():
    if not os.path.exists(os.path.join(TMP_DIR, "sentences_detailed.tar.bz2")):
        pytest.fail("Test data missing. Please run `extract_test_data.py` first.")


def _load_sentence_groups(out_dir: str) -> dict[str, set[int]]:
    result: dict[str, set[int]] = {}
    for lang in ("eng", "jpn"):
        bank_dir = os.path.join(out_dir, f"dict_{lang}")
        if not os.path.isdir(bank_dir):
            continue
        for name in sorted(os.listdir(bank_dir)):
            if not (name.startswith("example_bank_") and name.endswith(".json")):
                continue
            with open(os.path.join(bank_dir, name), encoding="utf-8") as f:
                for item in json.load(f):
                    result[item["sentence"]] = set(item["groupIds"])
    return result


def _run(main_lang: str | None, out_dir: str) -> dict[str, set[int]]:
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    tatoeba_to_dadb.run_pipeline(
        target_langs=["eng", "jpn"], top_n=None, main_lang=main_lang,
        delete_unzipped=False, include_tags=False, tmp_dir=TMP_DIR, out_dir=out_dir,
    )
    return _load_sentence_groups(out_dir)


@pytest.fixture(scope="module")
def run_main_eng() -> dict[str, set[int]]:
    return _run("eng", os.path.join("tests", "out"))


@pytest.fixture(scope="module")
def run_main_jpn() -> set[str]:
    return set(_run("jpn", os.path.join("tests", "out_main_lang")))


@pytest.fixture(scope="module")
def run_no_main() -> set[str]:
    return set(_run(None, os.path.join("tests", "out_no_main_lang")))


@pytest.mark.parametrize("case", DEDUPLICATION_TEST_CASES, ids=lambda c: c["sentence"])
def test_deduplication(run_main_eng, case):
    bank_path = os.path.join("tests", "out", "dict_jpn", "example_bank_1.json")
    assert os.path.exists(bank_path), "Export bank not found."
    with open(bank_path, encoding="utf-8") as f:
        data = json.load(f)
    matches = [item for item in data if item["sentence"] == case["sentence"]]
    assert len(matches) == case["expected_count"], (
        f"{case['description']}: expected {case['expected_count']}, got {len(matches)}."
    )


@pytest.mark.parametrize("case", GROUP_ID_CASES, ids=lambda c: c["source"])
def test_group_ids(run_main_eng, case):
    groups = run_main_eng
    source = case["source"]
    assert source in groups, f"Source '{source}' not found."
    src = groups[source]

    present = [t for t in case["expected_targets"] if t in groups]
    for target in present:
        assert src & groups[target], (
            f"'{target}' should share at least one groupId with '{source}'"
        )
    # Expected targets must co-cluster with one another, not just with the source.
    for i, t1 in enumerate(present):
        for t2 in present[i + 1:]:
            assert groups[t1] & groups[t2], (
                f"Expected '{t1}' and '{t2}' to share at least one groupId."
            )
    for unexpected in case["unexpected_targets"]:
        if unexpected in groups:
            assert not src & groups[unexpected], (
                f"'{unexpected}' must NOT share any groupId with '{source}'"
            )


@pytest.mark.parametrize("case", MAIN_LANG_TEST_CASES, ids=lambda c: c["source"])
def test_main_lang_jpn(run_main_jpn, case):
    included = case["source"] in run_main_jpn
    if case["should_be_excluded"]:
        assert not included, f"'{case['source']}' should be EXCLUDED but was INCLUDED"
    else:
        assert included, f"'{case['source']}' should be INCLUDED but was EXCLUDED"


@pytest.mark.parametrize("case", MAIN_LANG_TEST_CASES, ids=lambda c: c["source"])
def test_no_main_lang(run_no_main, case):
    assert case["source"] in run_no_main, (
        f"'{case['source']}' should be INCLUDED when no main_lang is set"
    )


def test_no_duplicate_sentences_in_bank(run_main_eng):
    for lang in ("eng", "jpn"):
        path = os.path.join("tests", "out", f"dict_{lang}", "example_bank_1.json")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        sentences = [item["sentence"] for item in data]
        duplicates = [s for s in set(sentences) if sentences.count(s) > 1]
        assert not duplicates, f"Duplicate sentences in {lang} bank: {duplicates}"


def test_self_anchor_is_always_in_own_groupids(run_main_eng):
    # Every entry must have ≥1 groupId so its own Tatoeba page card exists.
    bank_path = os.path.join("tests", "out", "dict_eng", "example_bank_1.json")
    with open(bank_path, encoding="utf-8") as f:
        data = json.load(f)
    for item in data:
        assert item["groupIds"], f"Entry has empty groupIds: {item['sentence']!r}"


def test_direct_translations_share_groupids(run_main_eng):
    groups = run_main_eng
    pairs = [
        ("I have to go to sleep.", "私は眠らなければなりません。"),
        ("What are you doing?",    "何してるの？"),
    ]
    for a, b in pairs:
        if a in groups and b in groups:
            assert groups[a] & groups[b], (
                f"Direct translations '{a}' and '{b}' must share a groupId; "
                f"got {groups[a]} vs {groups[b]}"
            )
