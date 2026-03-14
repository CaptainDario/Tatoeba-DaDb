# Tatoeba-DaExampleDicts

Repo to parse the Tatoeba corpus and extract DaDb Example Dicts for every language

## Installation

Make sure uv is installed on your system and run

```bash
uv sync
```

## Usage

To convert *every* language

```bash
uv run tatoeba_to_dadb.py
```

Other command line arguments

```bash
--langs (-l), space separated list of the languages (iso639-3) that should be extracted 
--delete-unzipped, delete the  
```