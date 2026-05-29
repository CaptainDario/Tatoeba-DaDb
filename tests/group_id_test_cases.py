# List of test cases for the translation graph building logic
TEST_CASES = [
    {
        "source": "Let's try something.",
        "expected_targets": ["何かしてみましょう。", "やってみましょう。"],
        "unexpected_targets": ["私は眠らなければなりません。", "何してるの？"]
    },
    {
        "source": "何かしてみましょう。",
        "expected_targets": ["やってみましょう。"],
        "unexpected_targets": ["私は眠らなければなりません。", "何してるの？"]
    },
    {
        "source": "I have to go to sleep.",
        "expected_targets": ["私は眠らなければなりません。", "そろそろ寝なくちゃ。", "もうそろそろ寝なくちゃ。"],
        "unexpected_targets": ["何してるの？", "何かしてみましょう。"]
    },
    {
        "source": "What are you doing?",
        "expected_targets": ["何してるの？", "何しているの？", "あなた、何してるの？", "何しているんですか？"],
        "unexpected_targets": ["私は眠らなければなりません。", "何かしてみましょう。"]
    }
]
