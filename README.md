# Tatoeba-DaExampleDicts

Repo to parse the Tatoeba corpus and extract DaDb Example Dicts for every language.

## Installation

Make sure [uv](https://github.com/astral-sh/uv) is installed on your system and run:

```bash
uv sync
```

## Usage

To fetch and convert **every** language in the database:

```bash
uv run tatoeba_to_dadb.py
```

### Command Line Arguments

* `--langs` (`-l`): Space-separated list of the language codes (ISO 639-3) that should be extracted. If omitted, builds all languages.
* `--main`: Can be used to specify a main language, this will be used to filter out examples from all other languages that do not have a translation in the specified `--main` language
* `--delete-unzipped`: Delete the raw, unzipped dictionary folders in the output directory after they have been successfully packaged into `.zip` archives. (Defaults to keeping them).
* `--include-tags`: Include tatoeba tag data (very low quality)

### Examples

**Standard run (all languages):**
```bash
uv run tatoeba_to_dadb.py
```

**Extract specific languages and clean up raw files:**
```bash
uv run tatoeba_to_dadb.py --langs eng jpn deu --delete-unzipped
```

**Extract the top 25 most frequent languages:**
```bash
uv run tatoeba_to_dadb.py --top 25
```

**Filter for translations related to a main language (e.g., only English-linked sentences):**
```bash
uv run tatoeba_to_dadb.py --main eng
```

**Include Tatoeba tags (noisy data) and use custom directories:**
```bash
uv run tatoeba_to_dadb.py --include-tags --tmp-dir ./custom_tmp --out-dir ./custom_out
```

**Full production-style filtered release:**
```bash
uv run tatoeba_to_dadb.py --langs eng jpn --main eng --delete-unzipped
```

## Output Location

Generated dictionaries are saved to the `/out/` directory as ready-to-import `.zip` files. Temporary Tatoeba data downloads are cached in the `/tmp/` directory so they don't have to be re-downloaded if you run the script multiple times within 24 hours.

## Format / Schema Used

The output follows [DaKanji example bank schema](https://github.com/CaptainDario/DaKanji/blob/refactor-v4/dart_packages/da_db/schemata/examples/example_banks/README.md).

## Tests

Run the tests with

```bash
uv run pytest tests
```
