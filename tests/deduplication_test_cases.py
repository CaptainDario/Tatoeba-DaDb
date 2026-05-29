# List of test cases for strict identical sentence deduplication
DEDUPLICATION_TEST_CASES = [
    {
        "sentence": "何しているの？",
        "expected_count": 1,
        "description": "Identical strings (IDs 11630868 and 12221481) must be merged"
    },
    {
        "sentence": "何してるの？",
        "expected_count": 1, 
        "description": "Unique string must exist as its own entry"
    },
    {
        "sentence": "何をしているの。",
        "expected_count": 1,
        "description": "Sentences differing only by punctuation must NOT be merged with '？' version"
    }
]
