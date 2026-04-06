import os
import re
import asyncio
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
import edge_tts

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL      = os.environ["NEON_POSTGRES_URL"]
GITHUB_TOKEN  = os.environ["GITHUB_TOKEN"]   # built-in, no setup needed
GITHUB_REPO   = os.environ["GITHUB_REPOSITORY"]  # built-in, e.g. "youruser/esg-digest"
VOICE         = "en-GB-RyanNeural"

def get_conn():
    return psycopg2.connect(NEON_URL)

# ─── STEP 1: FETCH LATEST UNPROCESSED DIGEST ──────────────

def fetch_latest_digest():
    now  = datetime.now(timezone.utc)
    iso  = now.isocalendar()
    week, year = iso[1], iso[0]
    print(f"Fetching digest for week {week}/{year}...")
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT id, title, script, week_number, year
        FROM digests
        WHERE week_number = %s AND year = %s AND audio_url IS NULL
    """, (week, year))
    digest = cur.fetchone()
    cur.close(); conn.close()
    if not digest:
        print("No unprocessed digest found.")
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

# ─── STEP 4: CREATE GITHUB RELEASE + UPLOAD MP3 ───────────

def upload_to_github_release(filename, digest):
    tag     = f"week-{digest['week_number']}-{digest['year']}"
    title   = digest["title"]
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    # Create the release
    print(f"Creating GitHub Release: {tag}...")
    r = requests.post(
        f"https://api.github.com/repos/{GITHUB_REPO}/releases",
        headers=headers,
        json={
            "tag_name": tag,
            "name": title,
            "body": digest.get("title", ""),
            "draft": False,
            "prerelease": False
        }
    )
    r.raise_for_status()
    release     = r.json()
    upload_url  = release["upload_url"].replace("{?name,label}", "")
    release_url = release["html_url"]

    # Upload MP3 as release asset
    print(f"Uploading MP3 to release...")
    with open(filename, "rb") as f:
        mp3_data = f.read()

    r = requests.post(
        upload_url,
        headers={
            **headers,
            "Content-Type": "audio/mpeg"
        },
        params={"name": filename},
        data=mp3_data
    )
    r.raise_for_status()
    asset     = r.json()
    audio_url = asset["browser_download_url"]

    print(f"  ✓ MP3 uploaded: {audio_url}")
    return audio_url, release_url

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
    filename            = generate_audio(digest)
    audio_url, rel_url  = upload_to_github_release(filename, digest)
    save_audio_url(digest["id"], audio_url)
    print(f"\n✅ Done! Release: {rel_url}")
    print(f"   Audio:   {audio_url}")

if __name__ == "__main__":
    run()
