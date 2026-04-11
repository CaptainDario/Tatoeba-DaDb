"""Generate a markdown release body from out/stats.json and write the full
language overview to LANGUAGES.md at the repository root.

The release notes show only the top TOP_N languages by sentence count.
A link to LANGUAGES.md (on the main branch) is appended for the full list.
"""
import json
import pathlib
import sys

TOP_N = 100
GITHUB_REPO = "CaptainDario/Tatoeba-DaDb"

stats_path = pathlib.Path("out/stats.json")
if not stats_path.exists():
    print("out/stats.json not found.", file=sys.stderr)
    sys.exit(1)

counts: dict[str, int] = json.loads(stats_path.read_text(encoding="utf-8"))
total_langs = len(counts)
total_sentences = sum(counts.values())

ranked = sorted(counts.items(), key=lambda x: x[1], reverse=True)
top = ranked[:TOP_N]

# --- Full language overview written to repo root ---
full_lines = [
    "# Tatoeba DaDb - Language Overview\n",
    f"**{total_langs} languages** · **{total_sentences:,} sentences** total\n",
    "| # | Language | Sentences |",
    "| ---: | --- | ---: |",
]
for i, (lang, n) in enumerate(ranked, 1):
    full_lines.append(f"| {i} | `{lang}` | {n:,} |")

pathlib.Path("LANGUAGES.md").write_text("\n".join(full_lines) + "\n", encoding="utf-8")
print(f"Wrote full language overview ({total_langs} languages) to LANGUAGES.md")

# --- Release notes (top 100 only) ---
full_url = f"https://github.com/{GITHUB_REPO}/blob/main/LANGUAGES.md"

note_lines = [
    f"**{total_langs} languages** · **{total_sentences:,} sentences** total\n",
    f"Top {TOP_N} languages by sentence count:\n",
    "| # | Language | Sentences |",
    "| ---: | --- | ---: |",
]
for i, (lang, n) in enumerate(top, 1):
    note_lines.append(f"| {i} | `{lang}` | {n:,} |")

note_lines.append(f"\n[Full language list]({full_url})")

notes = "\n".join(note_lines) + "\n"
pathlib.Path("out/release_notes.md").write_text(notes, encoding="utf-8")
print(f"Wrote release notes (top {TOP_N} languages) to out/release_notes.md")
