import os
import requests
import logging
import psycopg2
from datetime import datetime, timezone
from xml.sax.saxutils import escape

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL = os.environ.get("NEON_POSTGRES_URL", "")
if not NEON_URL:
    raise ValueError("NEON_POSTGRES_URL not found in environment variables.")

PODCAST_TITLE = "The ESG and Climate Briefing"
PODCAST_DESCRIPTION = "Your weekly AI-generated digest of the most important developments in sustainability, climate finance, carbon accounting, and non-financial reporting."
PODCAST_AUTHOR = "ESG Digest"
PODCAST_EMAIL = "parciald2012@gmail.com"
PODCAST_LANGUAGE = "en-gb"
PODCAST_CATEGORY = "Business"
PODCAST_SUBCATEGORY = "Non-Profit"
PODCAST_ARTWORK = "https://placehold.co/1400x1400/166534/ffffff/png?text=ESG+Briefing"
FEED_URL = "https://your-podcast-feed-url.com/rss.xml"

# ─── LOGGING ─────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── UTILITY FUNCTIONS ───────────────────────────────────

def table_exists() -> bool:
    """Check if the 'digests' table exists in Neon."""
    try:
        conn = psycopg2.connect(NEON_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'digests')")
        exists = cursor.fetchone()[0]
        conn.close()
        return exists
    except Exception as e:
        logger.warning(f"Table check failed: {e}")
        return False

def fetch_episodes() -> list:
    """Fetch all published episodes from the 'digests' table in Neon."""
    try:
        conn = psycopg2.connect(NEON_URL)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT title, summary, audio_bytes, week_number, year, created_at
            FROM digests
            WHERE audio_bytes IS NOT NULL
            ORDER BY created_at DESC
        """)

        columns = [desc[0] for desc in cursor.description]
        episodes = [dict(zip(columns, row)) for row in cursor.fetchall()]

        conn.close()
        return episodes
    except Exception as e:
        logger.error(f"Failed to fetch episodes: {e}")
        return []

def get_audio_size(url: str) -> str:
    """Get the size of an audio file from a URL."""
    try:
        r = requests.head(url, timeout=10)
        return r.headers.get("content-length", "0")
    except Exception:
        return "0"

def rfc2822(dt_str: str) -> str:
    """Format a datetime string to RFC2822 format."""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")

# ─── RSS GENERATION ───────────────────────────────────────

def build_rss(episodes: list) -> str:
    """Build the RSS feed XML."""
    pod_title = escape(PODCAST_TITLE)
    pod_desc = escape(PODCAST_DESCRIPTION)
    pod_author = escape(PODCAST_AUTHOR)
    pod_email = escape(PODCAST_EMAIL)

    items = ""
    for ep in episodes:
        if not ep.get("audio_bytes"):
            continue
        size = get_audio_size(ep["audio_url"]) if ep.get("audio_url") else str(len(ep["audio_bytes"]))

        item = f"""
        <item>
            <title>{escape(ep['title'])}</title>
            <description>{escape(ep['summary'])}</description>
            <enclosure url="{ep.get('audio_url', '')}" length="{size}" type="audio/mpeg"/>
            <pubDate>{rfc2822(ep['created_at'])}</pubDate>
            <guid>{ep.get('audio_url', '')}</guid>
        </item>
        """
        items += item

    rss = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
    xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
    xmlns:media="http://search.yahoo.com/mrss/"
    xmlns:content="http://purl.org/rss/1.0/modules/content/">
    <channel>
        <title>{pod_title}</title>
        <description>{pod_desc}</description>
        <link>{FEED_URL}</link>
        <language>{PODCAST_LANGUAGE}</language>
        <copyright>© {datetime.now().year} ESG Digest</copyright>
        <itunes:author>{pod_author}</itunes:author>
        <itunes:summary>{pod_desc}</itunes:summary>
        <itunes:explicit>clean</itunes:explicit>
        <itunes:owner>
            <itunes:name>{pod_author}</itunes:name>
            <itunes:email>{pod_email}</itunes:email>
        </itunes:owner>
        <itunes:image href="{PODCAST_ARTWORK}"/>
        <itunes:category text="{PODCAST_CATEGORY}">
            <itunes:category text="{PODCAST_SUBCATEGORY}"/>
        </itunes:category>
        {items}
    </channel>
</rss>
"""
    return rss

# ─── MAIN ─────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"ESG RSS Generator — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    if not table_exists():
        print("❌ 'digests' table does not exist in Neon.")
        return

    episodes = fetch_episodes()
    if not episodes:
        print("❌ No episodes found.")
        return

    rss = build_rss(episodes)

    # Save to a file or serve directly
    with open("rss.xml", "w", encoding="utf-8") as f:
        f.write(rss)

    print("✅ RSS feed generated and saved to rss.xml")

if __name__ == "__main__":
    main()
