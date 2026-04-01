import os
import re
import asyncio
import requests
from datetime import datetime, timezone
import psycopg2  # for Neon
from supabase import create_client  # for storage only

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL = os.environ["NEON_POSTGRES_URL"]  # e.g., "postgres://user:pass@ep-cool-123456.../dbname"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
MISTRAL_KEY = os.environ["MISTRAL_API_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)  # Only for storage

# ─── STEP 1: FETCH LATEST UNPROCESSED DIGEST FROM NEON ──────────────

def fetch_latest_digest():
    now = datetime.now(timezone.utc)
    iso = now.isocalendar()
    week, year = iso[1], iso[0]

    print(f"Fetching digest for week {week}/{year}...")

    try:
        conn = psycopg2.connect(NEON_URL)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, title, script, week_number, year
            FROM digests
            WHERE week_number = %s AND year = %s AND audio_url IS NULL
        """, (week, year))

        digest = cursor.fetchone()
        if not digest:
            print("No unprocessed digest found for this week.")
            return None

        # Convert to dict for compatibility
        digest = {
            "id": digest[0],
            "title": digest[1],
            "script": digest[2],
            "week_number": digest[3],
            "year": digest[4]
        }
        print(f"Found: {digest['title']}")
        return digest

    except Exception as e:
        print(f"❌ Neon Error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

# ─── STEP 2: CLEAN SCRIPT FOR SPEECH ──────────────────────

def clean_script(script):
    script = re.sub(r'\[PAUSE\]', '... ', script)
    script = re.sub(r'\[INTRO\]|\[OUTRO\]|\[SECTIONS?\]', '', script)
    script = re.sub(r'\s+', ' ', script).strip()
    return script

# ─── STEP 3: GENERATE AUDIO WITH MISTRAL VOXSTRAL TTS ─────────────────

def generate_audio(digest):
    script = clean_script(digest["script"])
    filename = f"digest_w{digest['week_number']}_{digest['year']}.mp3"

    print(f"Generating audio ({len(script)} chars) with Mistral Voxstral...")
    headers = {
        "Authorization": f"Bearer {MISTRAL_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "voxtral-mini-tts-2603",
        "input": script,
        "response_format": "mp3"
    }

    response = requests.post(
        "https://api.mistral.ai/v1/audio/speech",
        headers=headers,
        json=payload,
        timeout=120
    )
    response.raise_for_status()

    with open(filename, "wb") as f:
        f.write(response.content)

    size_kb = os.path.getsize(filename) // 1024
    print(f"  ✓ Audio saved: {filename} ({size_kb} KB)")
    return filename

# ─── STEP 4: UPLOAD TO SUPABASE STORAGE ───────────────────

def upload_audio(filename, digest):
    print(f"Uploading {filename} to Supabase Storage...")

    storage_path = f"episodes/{filename}"

    with open(filename, "rb") as f:
        audio_bytes = f.read()

    supabase.storage.from_("podcasts").upload(
        path=storage_path,
        file=audio_bytes,
        file_options={"content-type": "audio/mpeg"}
    )

    public_url = supabase.storage.from_("podcasts").get_public_url(storage_path)

    try:
        conn = psycopg2.connect(NEON_URL)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE digests
            SET audio_url = %s
            WHERE id = %s
        """, (public_url, digest["id"]))
        conn.commit()
        print(f"  ✓ Public URL: {public_url}")
    except Exception as e:
        print(f"❌ Neon Update Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

    return public_url

# ─── MAIN ─────────────────────────────────────────────────

def run():
    print("=" * 60)
    print(f"ESG TTS Generator — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    digest = fetch_latest_digest()
    if not digest:
        return

    filename = generate_audio(digest)
    public_url = upload_audio(filename, digest)

    print(f"\n✅ Done! Listen at: {public_url}")

if __name__ == "__main__":
    run()
