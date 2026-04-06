import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL     = os.environ["NEON_POSTGRES_URL"]
HEADERS      = {"User-Agent": "Mozilla/5.0 (compatible; ESG-Digest-Bot/1.0)"}

# Use TARGET_WEEK/YEAR if set (manual run), otherwise current week
_now         = datetime.now(timezone.utc)
_iso         = _now.isocalendar()
TARGET_WEEK  = int(os.environ.get("TARGET_WEEK", _iso[1]))
TARGET_YEAR  = int(os.environ.get("TARGET_YEAR", _iso[0]))

# Scrape articles from the full target week
ONE_WEEK_AGO = datetime(TARGET_YEAR, 1, 1, tzinfo=timezone.utc) + \
               timedelta(weeks=TARGET_WEEK - 1) - timedelta(days=7)

# ─── DB HELPER ────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(NEON_URL)

def save_article(source_label, title, url, published_at, body_text):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id FROM articles WHERE url = %s", (url,))
        if cur.fetchone():
            print(f"  ~ Skipping (exists): {title[:60]}")
            cur.close(); conn.close()
            return
        now = datetime.now(timezone.utc)
        cur.execute("""
            INSERT INTO articles
                (source_label, title, url, published_at, body_text,
                 processed, week_number, year, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            source_label, title, url,
            published_at.isoformat() if published_at else None,
            body_text[:50000], False,
            TARGET_WEEK, TARGET_YEAR,
            datetime.now(timezone.utc).isoformat()
        ))
        conn.commit()
        print(f"  ✓ Saved: {title[:70]}")
    except Exception as e:
        print(f"  ✗ DB error: {e}")
    finally:
        cur.close(); conn.close()

def fetch_article_body(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
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
            entry.get("summary", ""), "html.parser"
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

# ─── 4. GHG PROTOCOL ─────────────────────────────────────

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
    base  = "https://climate-adapt.eea.europa.eu"
    print(f"\n→ {label}")
    try:
        r = requests.get(f"{base}/en/news-archive", headers=HEADERS, timeout=45)
    except requests.exceptions.Timeout:
        print("  ⚠ Climate-ADAPT timed out — skipping.")
        return
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    for a in soup.select("a[href*='/news-archive/']"):
        title = a.get_text(strip=True)
        href  = a.get("href", "")
        if not title or len(title) < 15 or href in seen:
            continue
        seen.add(href)
        url = href if href.startswith("http") else base + href
        try:
            body = fetch_article_body(url)
        except Exception:
            body = title
        save_article(label, title, url, datetime.now(timezone.utc), body)

# ─── 6. CARBON BRIEF DEBRIEFED ───────────────────────────

def fetch_carbonbrief():
    label = "Carbon Brief / DeBriefed"
    print(f"\n→ {label}")
    feed = feedparser.parse("https://www.carbonbrief.org/feed/")
    for entry in feed.entries:
        tags = [t.get("term", "").lower() for t in entry.get("tags", [])]
        if "debriefed" not in tags and "debriefed" not in entry.get("link", "").lower():
            continue
        pub = entry.get("published_parsed")
        pub_dt = datetime(*pub[:6], tzinfo=timezone.utc) if pub else None
        if pub_dt and pub_dt < ONE_WEEK_AGO:
            continue
        body = BeautifulSoup(
            entry.get("summary", ""), "html.parser"
        ).get_text(separator=" ", strip=True)
        save_article(label, entry.title, entry.link, pub_dt, body)

# ─── 7. KOLUM CBAM ───────────────────────────────────────

def fetch_kolum():
    label = "Kolum CBAM Weekly"
    print(f"\n→ {label}")
    try:
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
            print("  ⚠ Kolum appears JS-rendered — content too short.")
    except Exception as e:
        print(f"  ✗ Error: {e}")

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
    fetch_carbonbrief()
    fetch_kolum()
    print("\n✅ Done.")

if __name__ == "__main__":
    run()
