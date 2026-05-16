# List of test cases for the translation graph building logic
TEST_CASES = [
    {
        "source": "Let's try something.",
        "expected_targets": ["何かしてみましょう。"],
        "unexpected_targets": ["やってみましょう。", "私は眠らなければなりません。", "何してるの？"]
    },
    {
        "source": "I have to go to sleep.",
        "expected_targets": ["私は眠らなければなりません。"],
        "unexpected_targets": ["何してるの？", "何かしてみましょう。"]
    },
    {
        "source": "What are you doing?",
        "expected_targets": ["何してるの？"],
        "unexpected_targets": ["私は眠らなければなりません。", "何かしてみましょう。"]
    }
]
