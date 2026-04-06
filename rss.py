import os
import requests
import psycopg2
from datetime import datetime, timezone
from xml.sax.saxutils import escape

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL       = os.environ["NEON_POSTGRES_URL"]
GITHUB_TOKEN   = os.environ["GITHUB_TOKEN"]
GITHUB_REPO    = os.environ["GITHUB_REPOSITORY"]

PODCAST_TITLE       = "The ESG and Climate Briefing"
PODCAST_DESCRIPTION = "Your weekly AI-generated digest of the most important developments in sustainability, climate finance, carbon accounting, and non-financial reporting."
PODCAST_AUTHOR      = "ESG Digest"
PODCAST_EMAIL       = "your@email.com"   # ← replace with your email
PODCAST_LANGUAGE    = "en-gb"
PODCAST_CATEGORY    = "Business"
PODCAST_SUBCATEGORY = "Non-Profit"
PODCAST_ARTWORK     = "https://placehold.co/1400x1400/166534/ffffff/png?text=ESG+Briefing"

def get_conn():
    return psycopg2.connect(NEON_URL)

# ─── STEP 1: FETCH ALL EPISODES ───────────────────────────

def fetch_episodes():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT title, summary, audio_url, week_number, year, created_at
        FROM digests
        WHERE audio_url IS NOT NULL
        ORDER BY created_at DESC
    """)
    episodes = cur.fetchall()
    cur.close(); conn.close()
    return [dict(e) for e in episodes]

# ─── STEP 2: GET AUDIO FILE SIZE ──────────────────────────

def get_audio_size(url):
    try:
        r = requests.head(url, timeout=10)
        return r.headers.get("content-length", "0")
    except Exception:
        return "0"

# ─── STEP 3: FORMAT DATE ──────────────────────────────────

def rfc2822(dt_str):
    try:
        dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")

# ─── STEP 4: BUILD RSS ────────────────────────────────────

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
        title    = escape(ep.get("title") or f"Week {ep['week_number']}, {ep['year']}")
        summary  = escape(ep.get("summary") or "")
        audio    = ep["audio_url"]

        items += f"""
    <item>
      <title>{title}</title>
      <link>{audio}</link>
      <description>{summary}</description>
      <enclosure url="{audio}" length="{size}" type="audio/mpeg"/>
      <guid isPermaLink="false">{audio}</guid>
      <pubDate>{pub_date}</pubDate>
      <itunes:duration>1200</itunes:duration>
      <itunes:summary>{summary}</itunes:summary>
      <itunes:explicit>false</itunes:explicit>
    </item>"""

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
  xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
  xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>{pod_title}</title>
    <description>{pod_desc}</description>
    <link>{feed_url}</link>
    <language>{PODCAST_LANGUAGE}</language>
    <itunes:author>{pod_author}</itunes:author>
    <itunes:owner>
      <itunes:name>{pod_author}</itunes:name>
      <itunes:email>{pod_email}</itunes:email>
    </itunes:owner>
    <itunes:category text="{PODCAST_CATEGORY}">
      <itunes:category text="{PODCAST_SUBCATEGORY}"/>
    </itunes:category>
    <itunes:explicit>false</itunes:explicit>
    <itunes:type>episodic</itunes:type>
    <itunes:image href="{PODCAST_ARTWORK}"/>
    <lastBuildDate>{datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")}</lastBuildDate>
    {items}
  </channel>
</rss>""".strip()

# ─── STEP 5: UPLOAD RSS AS GITHUB RELEASE ASSET ───────────

def upload_rss(rss_content):
    tag     = "podcast-rss-feed"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    # Check if release already exists and delete it
    r = requests.get(
        f"https://api.github.com/repos/{GITHUB_REPO}/releases/tags/{tag}",
        headers=headers
    )
    if r.status_code == 200:
        release_id = r.json()["id"]
        requests.delete(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/{release_id}",
            headers=headers
        )
        # Also delete the tag
        requests.delete(
            f"https://api.github.com/repos/{GITHUB_REPO}/git/refs/tags/{tag}",
            headers=headers
        )

    # Create fresh release for the RSS feed
    r = requests.post(
        f"https://api.github.com/repos/{GITHUB_REPO}/releases",
        headers=headers,
        json={
            "tag_name": tag,
            "name": "Podcast RSS Feed",
            "body": "This release contains the latest podcast RSS feed. Do not delete.",
            "draft": False,
            "prerelease": False
        }
    )
    r.raise_for_status()
    upload_url = r.json()["upload_url"].replace("{?name,label}", "")

    # Upload feed.xml
    r = requests.post(
        upload_url,
        headers={**headers, "Content-Type": "text/xml; charset=utf-8"},
        params={"name": "feed.xml"},
        data=rss_content.encode("utf-8")
    )
    r.raise_for_status()
    feed_url = r.json()["browser_download_url"]
    print(f"  ✓ RSS feed uploaded: {feed_url}")
    return feed_url

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
    feed_url    = f"https://github.com/{GITHUB_REPO}/releases/download/podcast-rss-feed/feed.xml"
    rss_content = build_rss(episodes, feed_url)
    final_url   = upload_rss(rss_content)
    print(f"\n✅ RSS feed live at:\n   {final_url}")
    print(f"\n→ Paste this URL into Spotify for Podcasters.")

if __name__ == "__main__":
    run()
