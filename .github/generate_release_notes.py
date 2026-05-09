"""Generate a markdown release body from out/stats.json and write the full
language overview to LANGUAGES.md at the repository root.

The release notes show only the top TOP_N languages by sentence count.
A link to LANGUAGES.md (on the main branch) is appended for the full list.
The per-release stats are saved to stats/<tag>.json.
"""
import argparse
import json
import pathlib
import sys

TOP_N = 100
GITHUB_REPO = "CaptainDario/Tatoeba-DaDb"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-lang", help="Main language code if this is a filtered release")
    parser.add_argument("--tag", required=True, help="Release tag — used for naming the per-release stats file")
    args = parser.parse_args()

    stats_path = pathlib.Path("out/stats.json")
    if not stats_path.exists():
        print("out/stats.json not found.", file=sys.stderr)
        sys.exit(1)

    counts: dict[str, int] = json.loads(stats_path.read_text(encoding="utf-8"))
    total_langs = len(counts)
    total_sentences = sum(counts.values())

    ranked = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    top = ranked[:TOP_N]

    # --- Save per-release stats file ---
    stats_dir = pathlib.Path("stats")
    stats_dir.mkdir(exist_ok=True)
    stats_out = stats_dir / f"{args.tag}.json"
    stats_out.write_text(json.dumps(counts, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote per-release stats to {stats_out}")

    # --- Full language overview written to repo root (only for full release) ---
    if not args.main_lang:
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

    # --- Release notes (unified structure for both full and filtered releases) ---
    full_url = f"https://github.com/{GITHUB_REPO}/blob/main/LANGUAGES.md"
    stats_url = f"https://github.com/{GITHUB_REPO}/blob/main/stats/{args.tag}.json"

    note_lines = []

    # Optional filter notice for --main releases — same core structure underneath
    if args.main_lang:
        stable_url = f"https://github.com/{GITHUB_REPO}/releases/tag/latest-main-{args.main_lang}"
        note_lines.append(f"> Filtered release — only translations linked to `{args.main_lang}` are included.")
        note_lines.append(f"> Stable URL: [{stable_url}]({stable_url})\n")

    note_lines.extend([
        f"**{total_langs} languages** · **{total_sentences:,} sentences** total\n",
        f"Top {TOP_N} languages by sentence count:\n",
        "| # | Language | Sentences |",
        "| ---: | --- | ---: |",
    ])
    for i, (lang, n) in enumerate(top, 1):
        note_lines.append(f"| {i} | `{lang}` | {n:,} |")

    note_lines.append(f"\n[Full language list]({full_url}) · [Statistics]({stats_url})")

    notes = "\n".join(note_lines) + "\n"
    pathlib.Path("out/release_notes.md").write_text(notes, encoding="utf-8")
    print(f"Wrote release notes (top {TOP_N} languages) to out/release_notes.md")

if __name__ == "__main__":
    main()

