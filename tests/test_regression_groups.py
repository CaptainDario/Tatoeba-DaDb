"""Synthetic regression fixtures for the three reported bugs:
   1. もういいかい？ missing 用意はいいかい。 (shared eng pivot).
   2. いい匂い。 over-merging with いい香り (transitive closure).
   3. Time to eat! singleton card (un-canonicalised raw sid as groupId).
"""

from __future__ import annotations

import io
import json
import os
import sys
import tarfile

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import tatoeba_to_dadb


def _write_tar_bz2(path: str, member_name: str, content: str) -> None:
    data = content.encode("utf-8")
    with tarfile.open(path, "w:bz2") as tar:
        info = tarfile.TarInfo(name=member_name)
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))


def _build_fixture(tmp_dir: str,
                   sentences: list[tuple[int, str, str, str]],
                   sentence_links: list[tuple[int, int]]) -> None:
    # Audio/tags/skills files are stubbed empty so the pipeline finds the expected layout.
    os.makedirs(tmp_dir, exist_ok=True)
    sent_tsv = "\n".join(
        f"{sid}\t{lang}\t{text}\t{user}\t\\N\t0000-00-00 00:00:00"
        for sid, lang, text, user in sentences
    ) + "\n"
    _write_tar_bz2(os.path.join(tmp_dir, "sentences_detailed.tar.bz2"),
                   "sentences_detailed.csv", sent_tsv)

    link_lines = []
    for a, b in sentence_links:
        link_lines.append(f"{a}\t{b}")
        link_lines.append(f"{b}\t{a}")
    _write_tar_bz2(os.path.join(tmp_dir, "links.tar.bz2"),
                   "links.csv", "\n".join(link_lines) + "\n")

    for name in ("sentences_with_audio.tar.bz2", "user_languages.tar.bz2"):
        _write_tar_bz2(os.path.join(tmp_dir, name), name.replace(".tar.bz2", ".csv"), "")
    with open(os.path.join(tmp_dir, "users_sentences.csv"), "w", encoding="utf-8") as f:
        f.write("")


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


def _card_members(groups: dict[str, set[int]], anchor: int) -> set[str]:
    return {sentence for sentence, gids in groups.items() if anchor in gids}


@pytest.fixture(scope="module")
def regression_run(tmp_path_factory) -> dict[str, set[int]]:
    tmp_dir = str(tmp_path_factory.mktemp("regression_tmp"))
    out_dir = str(tmp_path_factory.mktemp("regression_out"))

    # jpn sids placed both above AND below the eng pivot to exercise the
    # min(neighbour) asymmetry the old algorithm had.
    sentences = [
        # "Are you ready?" cluster — bug 1.
        (100, "eng", "Are you ready?",       "u_eng"),
        (101, "eng", "Are you guys ready?",  "u_eng"),
        (200, "jpn", "もういいかい？",        "u_jpn"),
        ( 50, "jpn", "用意はいいかい。",      "u_jpn"),

        # Smell cluster — bug 2. いい香り has extras that must NOT leak into いい匂い's card.
        (300, "eng", "It smells good!",      "u_eng"),
        (310, "eng", "This smells good.",    "u_eng"),
        (311, "eng", "What a nice smell!",   "u_eng"),
        (320, "jpn", "いい匂い。",            "u_jpn"),
        (330, "jpn", "いい香り！",            "u_jpn"),

        # Eat cluster — bug 3. Duplicate-text rows on both sides exercise dedup.
        (400, "eng", "Time to eat!",         "u_eng"),
        (401, "eng", "Time to eat!",         "u_eng"),
        (410, "jpn", "食事ですよ。",          "u_jpn"),
        (411, "jpn", "食事ですよ。",          "u_jpn"),

        # Unrelated control.
        (500, "eng", "Hello.",                "u_eng"),
        (510, "jpn", "こんにちは。",          "u_jpn"),
    ]
    links = [
        (100, 200), (100,  50),
        (101,  50),                            # Are you guys ready? links only to 用意はいいかい
        (300, 320), (300, 330), (310, 320),
        (311, 330),                            # What a nice smell! linked only to いい香り
        (400, 410), (401, 411),                # cross-dedup links
        (500, 510),
    ]
    _build_fixture(tmp_dir, sentences, links)

    tatoeba_to_dadb.run_pipeline(
        target_langs=["eng", "jpn"], top_n=None, main_lang="jpn",
        delete_unzipped=False, include_tags=False,
        tmp_dir=tmp_dir, out_dir=out_dir,
    )
    return _load_sentence_groups(out_dir)


def test_bug1_paraphrase_jpn_share_via_eng_pivot(regression_run):
    s = regression_run
    shared = s["もういいかい？"] & s["用意はいいかい。"]
    assert shared, (
        f"Both jpn paraphrases of 'Are you ready?' must share its groupId; "
        f"got {s['もういいかい？']} vs {s['用意はいいかい。']}"
    )


def test_bug1_youi_self_card_matches_tatoeba_view(regression_run):
    s = regression_run
    expected = {"用意はいいかい。", "Are you ready?", "Are you guys ready?"}
    matching = [g for g in s["用意はいいかい。"] if _card_members(s, g) == expected]
    assert len(matching) == 1, (
        f"Expected exactly one of 用意はいいかい's cards to be the Tatoeba view {expected}; "
        f"got cards { {g: _card_members(s, g) for g in s['用意はいいかい。']} }"
    )


def test_bug2_no_card_centred_on_iinioi_contains_iikaori(regression_run):
    s = regression_run
    for gid in s["いい匂い。"]:
        members = _card_members(s, gid)
        if "いい匂い。" in members and "いい香り！" in members:
            # Only legitimate co-occurrence is on the shared eng pivot's card.
            assert "It smells good!" in members, (
                f"Card {gid} merges both jpn but no shared pivot — phantom merge: {members}"
            )


def test_bug2_self_card_matches_tatoeba_view(regression_run):
    s = regression_run
    expected = {"いい匂い。", "It smells good!", "This smells good."}
    matching = [g for g in s["いい匂い。"] if _card_members(s, g) == expected]
    assert len(matching) == 1, (
        f"Expected exactly one Tatoeba-view card for いい匂い; "
        f"got cards { {g: _card_members(s, g) for g in s['いい匂い。']} }"
    )


def test_bug2_siblings_meet_on_pivot_card(regression_run):
    # They share via the eng pivot, not via each other's anchor.
    s = regression_run
    assert s["いい匂い。"] & s["いい香り！"]


def test_bug3_time_to_eat_dedups_and_shares_with_food_jpn(regression_run):
    s = regression_run
    assert "Time to eat!" in s and "食事ですよ。" in s
    assert s["Time to eat!"] & s["食事ですよ。"], (
        f"Direct translations should share a groupId; "
        f"got {s['Time to eat!']} vs {s['食事ですよ。']}"
    )


def test_no_singleton_cards_for_sentences_with_translations(regression_run):
    s = regression_run
    translated = [
        "Time to eat!", "食事ですよ。",
        "もういいかい？", "用意はいいかい。", "Are you ready?", "Are you guys ready?",
        "いい匂い。", "いい香り！", "It smells good!", "This smells good.", "What a nice smell!",
    ]
    for sentence in translated:
        if sentence not in s:
            continue
        for gid in s[sentence]:
            members = _card_members(s, gid)
            assert len(members) >= 2, (
                f"Singleton card: '{sentence}' is alone in card {gid} (members={members})"
            )


def test_unrelated_clusters_dont_merge(regression_run):
    s = regression_run
    hello = s["Hello."]
    assert s["こんにちは。"] == hello
    for unrelated in ("もういいかい？", "いい匂い。", "Time to eat!"):
        assert hello.isdisjoint(s[unrelated]), (
            f"'Hello.' cluster {hello} unexpectedly overlaps with '{unrelated}' {s[unrelated]}"
        )
