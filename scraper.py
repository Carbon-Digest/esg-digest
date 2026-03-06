import feedparser
import requests
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime, timezone, timedelta
import os

# ─── CONFIG ───────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

ONE_WEEK_AGO = datetime.now(timezone.utc) - timedelta(days=7)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ESG-Digest-Bot/1.0)"}

# ─── SUPABASE HELPER ──────────────────────────────────────

def save_article(source_label, title, url, published_at, body_text):
    """Insert article into Supabase, skip if URL already exists."""
    try:
        exists = supabase.table("articles").select("id").eq("url", url).execute()
        if exists.data:
            print(f"  ~ Skipping (exists): {title[:60]}")
            return
        now = datetime.now(timezone.utc)
        supabase.table("articles").insert({
            "source_label": source_label,
            "title": title,
            "url": url,
            "published_at": published_at.isoformat() if published_at else None,
            "body_text": body_text[:50000],
            "processed": False,
            "week_number": now.isocalendar()[1],
            "year": now.year
        }).execute()
        print(f"  ✓ Saved: {title[:70]}")
    except Exception as e:
        print(f"  ✗ DB error for {url}: {e}")

def fetch_article_body(url):
    """Fetch and extract plain text from an article URL."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        # Remove nav/footer noise
        for tag in soup(["nav", "footer", "script", "style", "header"]):
            tag.decompose()
        return soup.get_text(separator=" ", strip=True)
    except Exception:
        return ""

# ─── 1. DAVID CARLIN — RSS ────────────────────────────────

def fetch_substack():
    label = "David Carlin / Substack"
    print(f"\n→ {label}")
    feed = feedparser.parse("https://davidcarlin.substack.com/feed")
    for entry in feed.entries:
        pub = entry.get("published_parsed")
        pub_dt = datetime(*pub[:6], tzinfo=timezone.utc) if pub else None
        if pub_dt and pub_dt < ONE_WEEK_AGO:
            continue
        body = BeautifulSoup(
            entry.get("summary", ""),
            "html.parser"
        ).get_text(separator=" ", strip=True)
        save_article(label, entry.title, entry.link, pub_dt, body)

# ─── 2. SBTI NEWS ─────────────────────────────────────────

def fetch_sbti():
    label = "SBTi News"
    print(f"\n→ {label}")
    r = requests.get("https://sciencebasedtargets.org/news", headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    for a in soup.select("a[href*='/news/']"):
        title = a.get_text(strip=True)
        href = a.get("href", "")
        if not title or len(title) < 15 or href in seen:
            continue
        seen.add(href)
        url = href if href.startswith("http") else "https://sciencebasedtargets.org" + href
        body = fetch_article_body(url)
        save_article(label, title, url, datetime.now(timezone.utc), body)

# ─── 3. WRI NEWS ──────────────────────────────────────────

def fetch_wri():
    label = "WRI News"
    print(f"\n→ {label}")
    r = requests.get("https://www.wri.org/news", headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    for a in soup.select("a[href*='/news/']"):
        title = a.get_text(strip=True)
        href = a.get("href", "")
        if not title or len(title) < 15 or href in seen:
            continue
        seen.add(href)
        url = href if href.startswith("http") else "https://www.wri.org" + href
        body = fetch_article_body(url)
        save_article(label, title, url, datetime.now(timezone.utc), body)

# ─── 4. GHG PROTOCOL BLOG ────────────────────────────────

def fetch_ghgprotocol():
    label = "GHG Protocol"
    print(f"\n→ {label}")
    r = requests.get("https://ghgprotocol.org/blog", headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    for a in soup.select("a[href*='/blog/']"):
        title = a.get_text(strip=True)
        href = a.get("href", "")
        if not title or len(title) < 15 or href in seen:
            continue
        seen.add(href)
        url = href if href.startswith("http") else "https://ghgprotocol.org" + href
        body = fetch_article_body(url)
        save_article(label, title, url, datetime.now(timezone.utc), body)

# ─── 5. CLIMATE-ADAPT EEA ────────────────────────────────

def fetch_climate_adapt():
    label = "Climate-ADAPT (EEA)"
    base = "https://climate-adapt.eea.europa.eu"
    print(f"\n→ {label}")
    r = requests.get(f"{base}/en/news-archive", headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    for a in soup.select("a[href*='/news-archive/']"):
        title = a.get_text(strip=True)
        href = a.get("href", "")
        if not title or len(title) < 15 or href in seen:
            continue
        seen.add(href)
        url = href if href.startswith("http") else base + href
        # EEA news items include a summary on the list page — use that + body
        body = fetch_article_body(url)
        save_article(label, title, url, datetime.now(timezone.utc), body)

# ─── 6. KOLUM CBAM (best-effort) ─────────────────────────

def fetch_kolum():
    label = "Kolum CBAM Weekly"
    print(f"\n→ {label}")
    r = requests.get("https://www.kolum.earth/en/cbam/weekly", headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup(["nav", "footer", "script", "style", "header"]):
        tag.decompose()
    body = soup.get_text(separator=" ", strip=True)
    if len(body) > 300:
        save_article(label, f"CBAM Weekly — {datetime.now().strftime('%Y-W%V')}",
                     "https://www.kolum.earth/en/cbam/weekly",
                     datetime.now(timezone.utc), body)
    else:
        print("  ⚠ Kolum appears JS-rendered — content too short to use.")
        print("    → Consider copy-pasting the weekly text manually into Supabase,")
        print("      or upgrading this fetcher to use Playwright for JS rendering.")

# ─── 7. CARBON BRIEF — DEBRIEFED (RSS) ───────────────────

def fetch_carbonbrief():
    label = "Carbon Brief / DeBriefed"
    print(f"\n→ {label}")
    feed = feedparser.parse("https://www.carbonbrief.org/feed/")
    for entry in feed.entries:
        # Only pick up DeBriefed articles
        tags = [t.get("term", "").lower() for t in entry.get("tags", [])]
        if "debriefed" not in tags and "debriefed" not in entry.get("link", "").lower():
            continue
        pub = entry.get("published_parsed")
        pub_dt = datetime(*pub[:6], tzinfo=timezone.utc) if pub else None
        if pub_dt and pub_dt < ONE_WEEK_AGO:
            continue
        body = BeautifulSoup(
            entry.get("summary", ""),
            "html.parser"
        ).get_text(separator=" ", strip=True)
        save_article(label, entry.title, entry.link, pub_dt, body)

# ─── MAIN ─────────────────────────────────────────────────

def run():
    print("=" * 60)
    print(f"ESG Digest Scraper — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    fetch_substack()
    fetch_sbti()
    fetch_wri()
    fetch_ghgprotocol()
    fetch_climate_adapt()
    fetch_kolum()
    fetch_carbonbrief()
    print("\n✅ Done.")

if __name__ == "__main__":
    run()
