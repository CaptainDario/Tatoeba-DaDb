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

Extract only English and Japanese dictionaries and clean up the unzipped folders afterward:

```bash
uv run tatoeba_to_dadb.py -l eng jpn --delete-unzipped
```

Extract only German, keeping the unzipped folders:

```bash
uv run tatoeba_to_dadb.py -l deu
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
