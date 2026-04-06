import os
import re
import time
import asyncio
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
import edge_tts

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL      = os.environ["NEON_POSTGRES_URL"]
GITHUB_TOKEN  = os.environ["GITHUB_TOKEN"]
GITHUB_REPO   = os.environ["GITHUB_REPOSITORY"]
VOICE         = "en-GB-RyanNeural"

_now        = datetime.now(timezone.utc)
_iso        = _now.isocalendar()
TARGET_WEEK = int(os.environ.get("TARGET_WEEK", _iso[1]))
TARGET_YEAR = int(os.environ.get("TARGET_YEAR", _iso[0]))

# ─── DB ───────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(NEON_URL)

# ─── STEP 1: FETCH DIGEST ─────────────────────────────────

def fetch_latest_digest():
    print(f"Fetching digest for week {TARGET_WEEK}/{TARGET_YEAR}...")
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT id, title, script, week_number, year
        FROM digests
        WHERE week_number = %s AND year = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (TARGET_WEEK, TARGET_YEAR))
    digest = cur.fetchone()
    cur.close(); conn.close()
    if not digest:
        print("No digest found for this week.")
        return None
    print(f"Found: {digest['title']}")
    return dict(digest)

# ─── STEP 2: CLEAN SCRIPT ─────────────────────────────────

def clean_script(script):
    script = re.sub(r'\[PAUSE\]', '... ', script)
    script = re.sub(r'\[INTRO\]|\[OUTRO\]|\[SECTIONS?\]', '', script)
    script = re.sub(r'\s+', ' ', script).strip()
    return script

# ─── STEP 3: GENERATE AUDIO ───────────────────────────────

async def generate_audio_async(text, filename):
    communicate = edge_tts.Communicate(text, VOICE)
    await communicate.save(filename)

def generate_audio(digest):
    script   = clean_script(digest["script"])
    filename = f"digest_w{digest['week_number']}_{digest['year']}.mp3"
    print(f"Generating audio ({len(script)} chars) with {VOICE}...")
    asyncio.run(generate_audio_async(script, filename))
    size_kb = os.path.getsize(filename) // 1024
    print(f"  ✓ Audio saved: {filename} ({size_kb} KB)")
    return filename

# ─── STEP 4: UPLOAD TO GITHUB RELEASE ─────────────────────

def upload_to_github_release(filename, digest):
    tag     = f"week-{digest['week_number']}-{digest['year']}"
    title   = digest["title"]
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    # Delete existing release and tag if they exist
    r = requests.get(
        f"https://api.github.com/repos/{GITHUB_REPO}/releases/tags/{tag}",
        headers=headers
    )
    if r.status_code == 200:
        release_id = r.json()["id"]
        print(f"  Deleting existing release {tag}...")
        requests.delete(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/{release_id}",
            headers=headers
        )
    # Always try to delete the tag
    requests.delete(
        f"https://api.github.com/repos/{GITHUB_REPO}/git/refs/tags/{tag}",
        headers=headers
    )
    time.sleep(3)  # wait for GitHub to process deletion

    # Create fresh release
    print(f"Creating GitHub Release: {tag}...")
    r = requests.post(
        f"https://api.github.com/repos/{GITHUB_REPO}/releases",
        headers=headers,
        json={
            "tag_name": tag,
            "name": title,
            "body": title,
            "draft": False,
            "prerelease": False
        }
    )
    r.raise_for_status()
    release    = r.json()
    upload_url = release["upload_url"].replace("{?name,label}", "")
    rel_url    = release["html_url"]

    # Upload MP3
    print(f"Uploading MP3...")
    with open(filename, "rb") as f:
        mp3_data = f.read()

    r = requests.post(
        upload_url,
        headers={**headers, "Content-Type": "audio/mpeg"},
        params={"name": filename},
        data=mp3_data
    )
    r.raise_for_status()
    audio_url = r.json()["browser_download_url"]
    print(f"  ✓ MP3 uploaded: {audio_url}")
    return audio_url, rel_url

# ─── STEP 5: SAVE AUDIO URL TO NEON ──────────────────────

def save_audio_url(digest_id, audio_url):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        "UPDATE digests SET audio_url = %s WHERE id = %s",
        (audio_url, digest_id)
    )
    conn.commit()
    cur.close(); conn.close()
    print(f"  ✓ Audio URL saved to database.")

# ─── MAIN ─────────────────────────────────────────────────

def run():
    print("=" * 60)
    print(f"ESG TTS Generator — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    digest = fetch_latest_digest()
    if not digest:
        return
    filename           = generate_audio(digest)
    audio_url, rel_url = upload_to_github_release(filename, digest)
    save_audio_url(digest["id"], audio_url)
    print(f"\n✅ Done! Release: {rel_url}")
    print(f"   Audio:   {audio_url}")

if __name__ == "__main__":
    run()
