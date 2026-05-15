MAIN_LANG_TEST_CASES = [
    {
        "source": "They are too busy fighting against each other to care for common ideals.",
        "lang": "eng",
        "should_be_excluded": True  # Since we use --main jpn, English sentences with no Japanese counterpart must be excluded
    },
    {
        "source": "I am a flawed person, but these are flaws that can easily be fixed.",
        "lang": "eng",
        "should_be_excluded": True
    },
    {
        "source": "If you didn't know me that way then you simply didn't know me.",
        "lang": "eng",
        "should_be_excluded": True
    },
    {
        "source": "外国語がとっても楽しいと分った。",
        "lang": "jpn",
        "should_be_excluded": False # Since it is the main language, it is kept even without an English counterpart
    },
    {
        "source": "Let's try something.",
        "lang": "eng",
        "should_be_excluded": False # This has a Japanese counterpart ("何かしてみましょう。"), so it must be included
    }
]
