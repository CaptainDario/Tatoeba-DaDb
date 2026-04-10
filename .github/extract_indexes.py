"""Extract index.json from every dict_<lang>.zip into out/indexes/index_<lang>.json."""
import json
import pathlib
import sys
import zipfile

out_dir = pathlib.Path("out")
indexes_dir = out_dir / "indexes"
indexes_dir.mkdir(parents=True, exist_ok=True)

zip_files = sorted(out_dir.glob("*.zip"))
if not zip_files:
    print("No zip files found in out/.", file=sys.stderr)
    sys.exit(1)

extracted = 0
for zpath in zip_files:
    with zipfile.ZipFile(zpath) as zf:
        names = zf.namelist()
        candidates = [n for n in names if n.endswith("index.json")]
        if not candidates:
            print(f"No index.json found in {zpath.name}", file=sys.stderr)
            sys.exit(1)
        entry = "index.json" if "index.json" in candidates else sorted(candidates)[0]
        data = zf.read(entry)

    json.loads(data)  # validate

    lang = zpath.stem[5:] if zpath.stem.startswith("dict_") else zpath.stem
    (indexes_dir / f"index_{lang}.json").write_bytes(data)
    extracted += 1

print(f"Extracted {extracted} index file(s) to {indexes_dir}")
