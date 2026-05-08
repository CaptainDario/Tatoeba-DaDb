"""Upload release assets to a GitHub release with retry / rate-limit handling.

Usage:
    python3 .github/upload_assets.py <TAG> <REPO>

Uploads:
  - out/*.zip
  - out/indexes/index_*.json
"""
import subprocess
import sys
import time
import pathlib

MAX_ATTEMPTS = 10
INTER_FILE_DELAY = 1  # seconds between uploads (~60 req/min, safely under 80/min limit)

def upload_file(tag: str, repo: str, path: pathlib.Path) -> None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        result = subprocess.run(
            ["gh", "release", "upload", tag, str(path), "--repo", repo, "--clobber"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            return

        err = (result.stdout + result.stderr).strip()
        rate_limited = any(k in err.lower() for k in ("rate limit", "403", "429"))

        if rate_limited:
            wait = 60
            for token in err.split():
                if token.isdigit():
                    wait = int(token)
                    break
            print(f"    Rate limited (attempt {attempt}/{MAX_ATTEMPTS}), waiting {wait}s...")
            time.sleep(wait)
        else:
            print(f"    Error: {err}")
            if attempt == MAX_ATTEMPTS:
                sys.exit(1)
            time.sleep(5)

def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: upload_assets.py <TAG> <REPO>", file=sys.stderr)
        sys.exit(1)

    tag, repo = sys.argv[1], sys.argv[2]

    files = sorted(pathlib.Path("out").glob("*.zip")) + \
            sorted(pathlib.Path("out/indexes").glob("index_*.json"))

    if not files:
        print("No files to upload.", file=sys.stderr)
        sys.exit(1)

    total = len(files)
    print(f"Uploading {total} file(s)...")
    for i, f in enumerate(files):
        print(f"  [{i+1}/{total}] {f.name}")
        upload_file(tag, repo, f)
        if i + 1 < total:
            time.sleep(INTER_FILE_DELAY)

    print("Done.")

if __name__ == "__main__":
    main()
