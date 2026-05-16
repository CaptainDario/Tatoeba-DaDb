import os
import time
import urllib.request
import socket
import tarfile
import io
import json
import zipfile
import shutil
import argparse
import sys
from collections import defaultdict

# Config
BASE_URL = "https://downloads.tatoeba.org/exports/"
CHUNK_SIZE = 25000
GITHUB_USER = "CaptainDario"
GITHUB_REPO = "Tatoeba-DaDb"

# --- UTILS ---

def download_data(include_tags, tmp_dir):
    """Downloads files with 24h caching. Skips tags file if not requested."""
    os.makedirs(tmp_dir, exist_ok=True)
    one_day_seconds = 24 * 60 * 60
    
    def download_with_retry(url, dest, retries=10, connect_timeout=20,
                            min_speed_bps=50 * 1024, speed_window=15):
        """Resumable download with minimum-speed enforcement.

        If throughput drops below `min_speed_bps` bytes/s for `speed_window`
        consecutive seconds the chunk loop raises TimeoutError and the retry
        logic resumes from the partial file using an HTTP Range request.
        """
        CHUNK = 1024 * 64
        for attempt in range(1, retries + 1):
            try:
                resume_pos = os.path.getsize(dest) if os.path.exists(dest) else 0
                headers = {"User-Agent": "Mozilla/5.0"}
                if resume_pos:
                    headers["Range"] = f"bytes={resume_pos}-"
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=connect_timeout) as resp:
                    if resp.status == 200 and resume_pos:
                        resume_pos = 0  # server ignored Range, restart
                    total = resume_pos + int(resp.headers.get("Content-Length", 0))
                    downloaded = resume_pos
                    last_pct = -1
                    window_start = time.monotonic()
                    window_bytes = 0
                    mode = "ab" if resume_pos else "wb"
                    with open(dest, mode) as out:
                        while True:
                            chunk = resp.read(CHUNK)
                            if not chunk:
                                break
                            out.write(chunk)
                            downloaded += len(chunk)
                            window_bytes += len(chunk)
                            elapsed = time.monotonic() - window_start
                            if elapsed >= speed_window:
                                speed = window_bytes / elapsed
                                if speed < min_speed_bps:
                                    raise TimeoutError(
                                        f"speed {speed/1024:.1f} KB/s below "
                                        f"minimum {min_speed_bps//1024} KB/s"
                                    )
                                window_start = time.monotonic()
                                window_bytes = 0
                            if total > 0:
                                pct = min(100, int(downloaded * 100 / total))
                                if pct != last_pct:
                                    last_pct = pct
                                    print(f"   -> Downloading: {pct}%")
                return  # success
            except (OSError, TimeoutError, socket.timeout) as exc:
                print(f"   -> Attempt {attempt}/{retries} failed at "
                      f"{os.path.getsize(dest) if os.path.exists(dest) else 0} bytes: {exc}")
                if attempt == retries:
                    raise
                wait = min(5 * attempt, 30)
                print(f"   -> Resuming in {wait}s...")
                time.sleep(wait)

    targets = {
        os.path.join(tmp_dir, "sentences_detailed.tar.bz2"): BASE_URL + "sentences_detailed.tar.bz2",
        os.path.join(tmp_dir, "links.tar.bz2"): BASE_URL + "links.tar.bz2",
        os.path.join(tmp_dir, "sentences_with_audio.tar.bz2"): BASE_URL + "sentences_with_audio.tar.bz2",
        os.path.join(tmp_dir, "user_languages.tar.bz2"): BASE_URL + "user_languages.tar.bz2",
        os.path.join(tmp_dir, "users_sentences.csv"): BASE_URL + "users_sentences.csv"
    }

    if include_tags:
        targets[os.path.join(tmp_dir, "tags.tar.bz2")] = BASE_URL + "tags.tar.bz2"

    for fname, url in targets.items():
        if os.path.exists(fname):
            if (time.time() - os.path.getmtime(fname)) < one_day_seconds:
                print(f"[CACHE] '{fname}' is valid.")
                continue
            else:
                # Remove expired file so we don't try to resume it and get HTTP 416
                os.remove(fname)
        print(f"[DOWNLOAD] Fetching '{fname}'...")
        download_with_retry(url, fname)
        print()

def stream_tar_bz2(filename):
    """High-performance streaming for compressed exports."""
    with tarfile.open(filename, "r:bz2") as tar:
        for member in tar.getmembers():
            if member.isfile():
                f = tar.extractfile(member)
                if f:
                    text_f = io.TextIOWrapper(f, encoding='utf-8')
                    for line in text_f:
                        yield line.rstrip('\n')

# --- STEP FUNCTIONS ---

def parse_user_skills(tmp_dir):
    print("1. Parsing user skills...")
    skills = {}
    for line in stream_tar_bz2(os.path.join(tmp_dir, "user_languages.tar.bz2")):
        parts = line.split('\t')
        if len(parts) >= 3:
            skills[(parts[2], parts[0])] = parts[1]
    return skills

def parse_user_reviews(tmp_dir):
    print("2. Parsing user reviews...")
    revs = defaultdict(int)
    with open(os.path.join(tmp_dir, "users_sentences.csv"), 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if len(parts) >= 3:
                revs[int(parts[1])] += int(parts[2])
    return revs

def parse_tags(tmp_dir):
    print("3. Parsing sentence tags (Optional)...")
    s_tags = defaultdict(list)
    unique_set = set()
    fname = os.path.join(tmp_dir, "tags.tar.bz2")
    if not os.path.exists(fname):
        return s_tags, unique_set
        
    for line in stream_tar_bz2(fname):
        parts = line.split('\t')
        if len(parts) >= 2:
            sid, tname = int(parts[0]), parts[1].strip()
            # Basic noise filter to exclude some trash tags
            if check_bad_tag(tname):
                if tname not in s_tags[sid]:
                    s_tags[sid].append(tname)
                unique_set.add(tname)
    return s_tags, unique_set

def check_bad_tag(tag) -> bool:

    if len(tag) > 30:
        return False
    
    if tag.lower().startswith("by "):
        return False

    return True

def parse_audio_meta(tmp_dir):
    print("4. Parsing audio metadata...")
    s_audio = defaultdict(list)
    creators, licenses = set(), set()
    for line in stream_tar_bz2(os.path.join(tmp_dir, "sentences_with_audio.tar.bz2")):
        parts = line.split('\t')
        if len(parts) >= 2:
            sid = int(parts[0])
            user = parts[2] if len(parts) > 2 else ""
            lic = parts[3] if len(parts) > 3 else ""
            s_audio[sid].append({"id": parts[1], "user": user, "lic": lic})
            if user: creators.add(user)
            if lic: licenses.add(lic)
    return s_audio, creators, licenses

def build_direct_links(tmp_dir):
    print("5. Mapping direct translations (Level 1 only)...")
    links = defaultdict(set)
    for line in stream_tar_bz2(os.path.join(tmp_dir, "links.tar.bz2")):
        parts = line.split('\t')
        if len(parts) >= 2:
            try:
                u, v = int(parts[0]), int(parts[1])
                links[u].add(v)
                links[v].add(u)
            except ValueError:
                pass
    return links

def count_languages(tmp_dir):
    """Pre-pass: return a dict of {lang: sentence_count} for all languages."""
    print("0. Counting sentences per language for top-N selection...")
    counts = defaultdict(int)
    for line in stream_tar_bz2(os.path.join(tmp_dir, "sentences_detailed.tar.bz2")):
        parts = line.split('\t')
        if len(parts) < 2: continue
        lang = parts[1]
        if lang == r'\N': continue
        counts[lang] += 1
    return counts

def collect_main_lang_groups(tmp_dir, main_lang, links):
    print(f"5b. Identifying sentences linked to main language '{main_lang}'...")
    linked_to_main = set()
    for line in stream_tar_bz2(os.path.join(tmp_dir, "sentences_detailed.tar.bz2")):
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        sid, lang = int(parts[0]), parts[1]
        if lang == main_lang:
            linked_to_main.add(sid)
            for neighbor in links.get(sid, []):
                linked_to_main.add(neighbor)
    return linked_to_main

# --- MAIN ENGINE ---

def run_pipeline(target_langs, top_n, main_lang, delete_unzipped, include_tags, tmp_dir, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    # If --top is set and --langs is not, determine top N languages by sentence count
    if top_n and not target_langs:
        lang_counts = count_languages(tmp_dir)
        ranked = sorted(lang_counts.items(), key=lambda x: x[1], reverse=True)
        target_langs = [lang for lang, _ in ranked[:top_n]]
        print(f"   Top {top_n} languages selected: {', '.join(target_langs)}")

    # Run parsing steps
    skills = parse_user_skills(tmp_dir)
    reviews = parse_user_reviews(tmp_dir)
    sentence_tags, unique_tags = parse_tags(tmp_dir) if include_tags else ({}, set())
    audio_meta, creators, licenses = parse_audio_meta(tmp_dir)
    
    allowed_langs = None
    if target_langs:
        allowed_langs = set(target_langs)
        if main_lang:
            allowed_langs.add(main_lang)
            
    links = build_direct_links(tmp_dir)

    valid_sentence_ids = collect_main_lang_groups(tmp_dir, main_lang, links) if main_lang else None

    # Create Tag Bank
    tag_bank = []
    for t in sorted(unique_tags):
        tag_bank.append([t, "sentence_tag", 1, f"Tatoeba: {t}", 0])
    for l in sorted(licenses):
        tag_bank.append([l, "audio_license", 2, f"License: {l}", 0])
    for c in sorted(creators):
        tag_bank.append([c, "audio_creator", 3, f"Voice: {c}", 0])

    print("6. Generating language chunks...")
    lang_states = {}
    total_processed = 0

    for line in stream_tar_bz2(os.path.join(tmp_dir, "sentences_detailed.tar.bz2")):
        parts = line.split('\t')
        if len(parts) < 4: continue
        
        sid, lang, text, user = int(parts[0]), parts[1], parts[2], parts[3]
        if lang == r'\N': continue  # skip Tatoeba null/unknown language
        if target_langs and lang not in target_langs: continue
        if valid_sentence_ids is not None and sid not in valid_sentence_ids: continue

        if lang not in lang_states:
            l_dir = os.path.join(out_dir, f"dict_{lang}")
            os.makedirs(l_dir, exist_ok=True)
            
            # Robust index.json
            with open(os.path.join(l_dir, "index.json"), "w", encoding="utf-8") as f:
                json.dump({
                    "title": f"Tatoeba Example Bank ({lang.upper()})",
                    "revision": f"tatoeba_{time.strftime('%Y%m%d')}",
                    "format": 3,
                    "sequenced": True,
                    "author": "Tatoeba.org Contributors",
                    "attribution": "Creative Commons Attribution 2.0 France (CC-BY 2.0 FR)",
                    "url": "https://tatoeba.org",
                    "description": f"Comprehensive example sentence bank for {lang} from the Tatoeba Project. Includes community-verified translations, audio download links, and contributor proficiency metrics.",
                    "sourceLanguage": lang,  # Identifying the bank's primary language
                    "isUpdatable": True,
                    "indexUrl": f"https://github.com/{GITHUB_USER}/{GITHUB_REPO}/releases/latest/download/index_{lang}.json",
                    "downloadUrl": f"https://github.com/{GITHUB_USER}/{GITHUB_REPO}/releases/latest/download/tatoeba_dadb_{lang}.zip"
                }, f, indent=2, ensure_ascii=False)

            with open(os.path.join(l_dir, "tag_bank_1.json"), "w", encoding="utf-8") as f:
                json.dump(tag_bank, f, ensure_ascii=False)

            f_chunk = open(os.path.join(l_dir, "example_bank_1.json"), "w", encoding="utf-8")
            f_chunk.write("[\n")
            lang_states[lang] = {"f": f_chunk, "count": 0, "total": 0, "idx": 1, "dir": l_dir, "first": True}

        state = lang_states[lang]
        if state["count"] >= CHUNK_SIZE:
            state["f"].write("\n]\n")
            state["f"].close()
            state["idx"] += 1
            state["count"] = 0
            state["first"] = True
            state["f"] = open(os.path.join(state["dir"], f"example_bank_{state['idx']}.json"), "w", encoding="utf-8")
            state["f"].write("[\n")

        # Prep JSON object
        stats = []
        if sid in reviews: stats.append({"statName": "review_score", "value": reviews[sid]})
        sk = skills.get((user, lang))
        if sk:
            skill_value = int(sk) if sk.isdigit() else 0
            skill_entry = {"statName": "user_skill", "value": skill_value}
            if str(skill_value) != str(sk):
                skill_entry["displayValue"] = str(sk)
            stats.append(skill_entry)

        # Deduplicate tags
        audios = []
        for a in audio_meta.get(sid, []):
            a_tags = [t for t in [a['user'], a['lic']] if t]
            audios.append({
                "url": f"https://tatoeba.org/audio/download/{a['id']}",
                "tags": list(dict.fromkeys(a_tags))
            })

        # Generate the list of groupIds for this sentence using min(sid, neighbor)
        my_links = links.get(sid, set())
        group_ids = sorted(list(set([min(sid, neighbor) for neighbor in my_links])))

        obj = {
            "groupIds": group_ids,
            "sentence": text,
            "tags": list(dict.fromkeys(sentence_tags.get(sid, []))),
            "stats": stats,
            "audios": audios
        }

        if not state["first"]: state["f"].write(",\n")
        state["f"].write("  " + json.dumps(obj, ensure_ascii=False))
        state["first"] = False
        state["count"] += 1
        state["total"] += 1
        total_processed += 1
        if total_processed % 100000 == 0:
            print(f"   ... Processed {total_processed} sentences")

    # Finalize files
    print("7. Zipping results...")
    lang_counts = {}
    for lang, state in lang_states.items():
        state["f"].write("\n]\n")
        state["f"].close()
        lang_counts[lang] = state["total"]
        z_path = os.path.join(out_dir, f"tatoeba_dadb_{lang}.zip")
        with zipfile.ZipFile(z_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(state["dir"]):
                for file in files:
                    zf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), state["dir"]))
        if delete_unzipped: shutil.rmtree(state["dir"])

    stats_path = os.path.join(out_dir, "stats.json")
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(lang_counts, f, indent=2, ensure_ascii=False)
    print(f"   Wrote sentence counts to {stats_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--langs', nargs='+')
    parser.add_argument('--top', type=int, default=None, help="Only process the top N most frequent languages (ignored when --langs is set)")
    parser.add_argument('--main', default=None, help="Main language code. Only sentences with a translation in this language are kept for other languages.")
    parser.add_argument('--delete-unzipped', action='store_true')
    parser.add_argument('--include-tags', action='store_true', help="Parse and include noisy Tatoeba tags")
    parser.add_argument('--tmp-dir', default="./tmp/", help="Temporary directory for downloads")
    parser.add_argument('--out-dir', default="./out/", help="Output directory for generated dictionaries")
    args = parser.parse_args()
    
    print("")
    print("========================================")
    print("TATOEBA TO DAKANJI DICTIONARY BUILDER")
    print("========================================")
    print(f"Target Languages: {', '.join(args.langs) if args.langs else f'top {args.top}' if args.top else 'ALL'}")
    print(f"Main Language:    {args.main if args.main else 'N/A (include all)'}")
    print(f"Include Tags:     {args.include_tags}")
    print(f"Delete Unzipped:  {args.delete_unzipped}\n")
    print("")

    download_data(args.include_tags, args.tmp_dir)
    run_pipeline(args.langs, args.top, args.main, args.delete_unzipped, args.include_tags, args.tmp_dir, args.out_dir)
