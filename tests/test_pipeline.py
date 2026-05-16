import os
import sys
import json
import shutil
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import tatoeba_to_dadb
from tests.group_id_test_cases import TEST_CASES as GROUP_ID_CASES
from tests.main_lang_test_cases import MAIN_LANG_TEST_CASES

TMP_DIR = os.path.join("tests", "data")

@pytest.fixture(scope="session", autouse=True)
def check_data():
    if not os.path.exists(os.path.join(TMP_DIR, "sentences_detailed.tar.bz2")):
        pytest.fail("Test data missing. Please run `extract_test_data.py` first.")

@pytest.fixture(scope="module")
def run_main_eng():
    out_dir = os.path.join("tests", "out")
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    tatoeba_to_dadb.run_pipeline(
        target_langs=["eng", "jpn"], top_n=None, main_lang="eng",
        delete_unzipped=False, include_tags=False, tmp_dir=TMP_DIR, out_dir=out_dir
    )
    
    sentence_to_groups = {}
    for lang in ["eng", "jpn"]:
        bank_path = os.path.join(out_dir, f"dict_{lang}", "example_bank_1.json")
        if os.path.exists(bank_path):
            with open(bank_path, "r", encoding="utf-8") as f:
                for item in json.load(f):
                    sentence_to_groups[item["sentence"]] = set(item["groupIds"])
    return sentence_to_groups

@pytest.fixture(scope="module")
def run_main_jpn():
    out_dir = os.path.join("tests", "out_main_lang")
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    tatoeba_to_dadb.run_pipeline(
        target_langs=["eng", "jpn"], top_n=None, main_lang="jpn",
        delete_unzipped=False, include_tags=False, tmp_dir=TMP_DIR, out_dir=out_dir
    )
    
    included_sentences = set()
    for lang in ["eng", "jpn"]:
        bank_path = os.path.join(out_dir, f"dict_{lang}", "example_bank_1.json")
        if os.path.exists(bank_path):
            with open(bank_path, "r", encoding="utf-8") as f:
                for item in json.load(f):
                    included_sentences.add(item["sentence"])
    return included_sentences

@pytest.fixture(scope="module")
def run_no_main():
    out_dir = os.path.join("tests", "out_no_main_lang")
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    tatoeba_to_dadb.run_pipeline(
        target_langs=["eng", "jpn"], top_n=None, main_lang=None,
        delete_unzipped=False, include_tags=False, tmp_dir=TMP_DIR, out_dir=out_dir
    )
    
    included_sentences = set()
    for lang in ["eng", "jpn"]:
        bank_path = os.path.join(out_dir, f"dict_{lang}", "example_bank_1.json")
        if os.path.exists(bank_path):
            with open(bank_path, "r", encoding="utf-8") as f:
                for item in json.load(f):
                    included_sentences.add(item["sentence"])
    return included_sentences


# Tests for group ID (using main=eng output)
@pytest.mark.parametrize("case", GROUP_ID_CASES, ids=lambda c: c["source"])
def test_group_ids(run_main_eng, case):
    sentence_to_groups = run_main_eng
    source = case["source"]
    assert source in sentence_to_groups, f"Source '{source}' not found."
    source_groups = sentence_to_groups[source]
    
    for expected in case["expected_targets"]:
        if expected in sentence_to_groups:
            assert source_groups.intersection(sentence_to_groups[expected]), f"Expected '{expected}' to share at least one groupId with '{source}'"
            
    for unexpected in case["unexpected_targets"]:
        if unexpected in sentence_to_groups:
            assert not source_groups.intersection(sentence_to_groups[unexpected]), f"Expected '{unexpected}' to NOT share any groupIds with '{source}'"


# Tests for main_lang=jpn
@pytest.mark.parametrize("case", MAIN_LANG_TEST_CASES, ids=lambda c: c["source"])
def test_main_lang_jpn(run_main_jpn, case):
    included_sentences = run_main_jpn
    source = case["source"]
    is_included = source in included_sentences
    if case["should_be_excluded"]:
        assert not is_included, f"'{source}' should be EXCLUDED but was INCLUDED"
    else:
        assert is_included, f"'{source}' should be INCLUDED but was EXCLUDED"


# Tests for main_lang=None
@pytest.mark.parametrize("case", MAIN_LANG_TEST_CASES, ids=lambda c: c["source"])
def test_no_main_lang(run_no_main, case):
    included_sentences = run_no_main
    source = case["source"]
    # With no main lang, everything should be included
    assert source in included_sentences, f"'{source}' should be INCLUDED but was EXCLUDED"
