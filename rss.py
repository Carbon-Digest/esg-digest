import os
import requests
from datetime import datetime, timezone
from xml.sax.saxutils import escape
from supabase import create_client

# ─── CONFIG ───────────────────────────────────────────────

SUPABASE_URL        = os.environ["SUPABASE_URL"]
SUPABASE_KEY        = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PODCAST_TITLE       = "The ESG and Climate Briefing"
PODCAST_DESCRIPTION = "Your weekly AI-generated digest of the most important developments in sustainability, climate finance, carbon accounting, and non-financial reporting."
PODCAST_AUTHOR      = "ESG Digest"
PODCAST_EMAIL       = "your@email.com"   # ← replace with your email
PODCAST_LANGUAGE    = "en-gb"
PODCAST_CATEGORY    = "Business"

# ─── STEP 1: FETCH ALL PUBLISHED EPISODES ─────────────────

def fetch_episodes():
    result = supabase.table("digests") \
        .select("title, summary, audio_url, week_number, year, created_at") \
        .not_.is_("audio_url", "null") \
        .order("created_at", desc=True) \
        .execute()
    return result.data

# ─── STEP 2: GET AUDIO FILE SIZE ──────────────────────────

def get_audio_size(url):
    try:
        r = requests.head(url, timeout=10)
        return r.headers.get("content-length", "0")
    except Exception:
        return "0"

# ─── STEP 3: FORMAT DATE FOR RSS ──────────────────────────

def rfc2822(dt_str):
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")

# ─── STEP 4: BUILD RSS XML ────────────────────────────────

def build_rss(episodes, feed_url):
    pod_title  = escape(PODCAST_TITLE)
    pod_desc   = escape(PODCAST_DESCRIPTION)
    pod_author = escape(PODCAST_AUTHOR)
    pod_email  = escape(PODCAST_EMAIL)

    items = ""
    for ep in episodes:
        if not ep.get("audio_url"):
            continue
        size     = get_audio_size(ep["audio_url"])
        pub_date = rfc2822(ep.get("created_at", ""))
        title    = escape(ep.get("title", f"Week {ep['week_number']}, {ep['year']}"))
        summary  = escape(ep.get("summary", ""))

        items += f"""
    <item>
      <title>{title}</title>
      <description>{summary}</description>
      <enclosure url="{ep['audio_url']}" length="{size}" type="audio/mpeg"/>
      <guid isPermaLink="false">{ep['audio_url']}</guid>
      <pubDate>{pub_date}</pubDate>
      <itunes:duration>1200</itunes:duration>
      <itunes:summary>{summary}</itunes:summary>
      <itunes:explicit>false</itunes:explicit>
    </item>"""

    rss = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
  xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
  xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>{pod_title}</title>
    <description>{pod_desc}</description>
    <link>{feed_url}</link>
    <language>{PODCAST_LANGUAGE}</language>
    <itunes:author>{pod_author}</itunes:author>
    <itunes:email>{pod_email}</itunes:email>
    <itunes:category text="{PODCAST_CATEGORY}"/>
    <itunes:explicit>false</itunes:explicit>
    <itunes:type>episodic</itunes:type>
    <lastBuildDate>{datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")}</lastBuildDate>
    {items}
  </channel>
</rss>"""
    return rss.strip()

# ─── STEP 5: UPLOAD RSS TO SUPABASE STORAGE ───────────────

def upload_rss(rss_content):
    storage_path = "rss/feed.xml"
    rss_bytes    = rss_content.encode("utf-8")

    try:
        supabase.storage.from_("podcasts").remove([storage_path])
    except Exception:
        pass

    supabase.storage.from_("podcasts").upload(
        path=storage_path,
        file=rss_bytes,
        file_options={"content-type": "text/xml; charset=utf-8"}
    )

    public_url = supabase.storage.from_("podcasts").get_public_url(storage_path)
    print(f"  ✓ RSS feed uploaded: {public_url}")
    return public_url

# ─── MAIN ─────────────────────────────────────────────────

def run():
    print("=" * 60)
    print(f"ESG RSS Generator — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    episodes = fetch_episodes()
    print(f"Found {len(episodes)} published episodes.")

    if not episodes:
        print("No episodes with audio yet. Exiting.")
        return

    feed_url    = supabase.storage.from_("podcasts").get_public_url("rss/feed.xml")
    rss_content = build_rss(episodes, feed_url)
    public_url  = upload_rss(rss_content)

    print(f"\n✅ RSS feed live at:\n   {public_url}")
    print(f"\n→ Paste this URL into Spotify for Podcasters.")

if __name__ == "__main__":
    run()
