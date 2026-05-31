# Two sentences share a groupId iff they're direct translations, or both directly
# translate the same pivot. Paths via non-allowed-lang intermediates (fra/deu) don't count.
TEST_CASES = [
    {
        "source": "Let's try something.",
        "expected_targets": ["何かしてみましょう。"],
        # やってみましょう only links via fra/deu — separate cluster.
        "unexpected_targets": ["私は眠らなければなりません。", "何してるの？", "やってみましょう。"]
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
