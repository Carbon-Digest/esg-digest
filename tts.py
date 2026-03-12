import os
import re
import asyncio
from datetime import datetime, timezone
from supabase import create_client
import edge_tts

# ─── CONFIG ───────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# British male neural voice — free, no account needed
VOICE    = "en-GB-RyanNeural"

# ─── STEP 1: FETCH LATEST UNPROCESSED DIGEST ──────────────

def fetch_latest_digest():
    now = datetime.now(timezone.utc)
    iso = now.isocalendar()
    week, year = iso[1], iso[0]

    print(f"Fetching digest for week {week}/{year}...")

    result = supabase.table("digests") \
        .select("id, title, script, week_number, year") \
        .eq("week_number", week) \
        .eq("year", year) \
        .is_("audio_url", "null") \
        .execute()

    if not result.data:
        print("No unprocessed digest found for this week.")
        return None

    digest = result.data[0]
    print(f"Found: {digest['title']}")
    return digest

# ─── STEP 2: CLEAN SCRIPT FOR SPEECH ──────────────────────

def clean_script(script):
    script = re.sub(r'\[PAUSE\]', '... ', script)
    script = re.sub(r'\[INTRO\]|\[OUTRO\]|\[SECTIONS?\]', '', script)
    script = re.sub(r'\s+', ' ', script).strip()
    return script

# ─── STEP 3: GENERATE AUDIO WITH EDGE TTS ─────────────────

async def generate_audio_async(text, filename):
    communicate = edge_tts.Communicate(text, VOICE)
    await communicate.save(filename)

def generate_audio(digest):
    script = clean_script(digest["script"])
    filename = f"digest_w{digest['week_number']}_{digest['year']}.mp3"

    print(f"Generating audio ({len(script)} chars) with {VOICE}...")
    asyncio.run(generate_audio_async(script, filename))

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

    supabase.table("digests") \
        .update({"audio_url": public_url}) \
        .eq("id", digest["id"]) \
        .execute()

    print(f"  ✓ Public URL: {public_url}")
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
