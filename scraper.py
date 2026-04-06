import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import psycopg2

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL     = os.environ["NEON_POSTGRES_URL"]
HEADERS      = {"User-Agent": "Mozilla/5.0 (compatible; ESG-Digest-Bot/1.0)"}

_now         = datetime.now(timezone.utc)
_iso         = _now.isocalendar()
TARGET_WEEK  = int(os.environ.get("TARGET_WEEK", _iso[1]))
TARGET_YEAR  = int(os.environ.get("TARGET_YEAR", _iso[0]))

ONE_WEEK_AGO = datetime(TARGET_YEAR, 1, 1, tzinfo=timezone.utc) + \
               timedelta(weeks=TARGET_WEEK - 1) - timedelta(days=7)

MAX_ARTICLES_PER_SOURCE = 5

# ─── DB HELPER ────────────────────────────────────────────

# ─── DB HELPER ────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(NEON_URL)

def get_existing_urls(conn):
    """Fetch all existing URLs in one query to avoid per-article DB lookups."""
    cur = conn.cursor()
    cur.execute("SELECT url FROM articles")
    urls = {row[0] for row in cur.fetchall()}
    cur.close()
    return urls

def get_source_counts(conn):
    """Fetch article counts per source this week in one query."""
    cur = conn.cursor()
    cur.execute("""
        SELECT source_label, COUNT(*) FROM articles
        WHERE week_number = %s GROUP BY source_label
    """, (TARGET_WEEK,))
    counts = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return counts

def save_all_articles(articles):
    """Save all articles in a single DB connection with one commit."""
    conn = get_conn()
    cur  = conn.cursor()
    existing_urls  = get_existing_urls(conn)
    source_counts  = get_source_counts(conn)
    saved = 0

    for source_label, title, url, published_at, body_text in articles:
        if url in existing_urls:
            continue
        if source_counts.get(source_label, 0) >= MAX_ARTICLES_PER_SOURCE:
            continue
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
        existing_urls.add(url)
        source_counts[source_label] = source_counts.get(source_label, 0) + 1
        print(f"  ✓ Saved [{source_label}]: {title[:60]}")
        saved += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nSaved {saved} new articles.")

def scrape_text(url):
    """Fetch plain text from a URL — used only for RSS sources with no body."""
    try:
        r    = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["nav", "footer", "script", "style", "header"]):
            tag.decompose()
        return soup.get_text(separator=" ", strip=True)[:50000]
    except Exception:
        return ""

# ─── SOURCES ──────────────────────────────────────────────
# Each fetch function collects articles and returns them as a list
# No DB writes inside — all writes happen at the end to avoid connection overload

def fetch_rss(label, feed_url, filter_fn=None):
    """Generic RSS fetcher. filter_fn(entry) returns True to include."""
    print(f"\n→ {label}")
    results = []
    try:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            pub    = entry.get("published_parsed")
            pub_dt = datetime(*pub[:6], tzinfo=timezone.utc) if pub else None
            if pub_dt and pub_dt < ONE_WEEK_AGO:
                continue
            if filter_fn and not filter_fn(entry):
                continue
            body = BeautifulSoup(
                entry.get("summary", ""), "html.parser"
            ).get_text(separator=" ", strip=True)
            results.append((label, entry.title, entry.link, pub_dt, body))
    except Exception as e:
        print(f"  ✗ Error: {e}")
    return results

def fetch_scrape(label, url, link_selector, base_url, timeout=20):
    """Generic page scraper — uses title + excerpt from listing page only, no individual fetches."""
    print(f"\n→ {label}")
    results = []
    try:
        r    = requests.get(url, headers=HEADERS, timeout=timeout)
        soup = BeautifulSoup(r.text, "html.parser")
        seen = set()
        for a in soup.select(link_selector):
            title = a.get_text(strip=True)
            href  = a.get("href", "")
            if not title or len(title) < 15 or href in seen:
                continue
            seen.add(href)
            full_url = href if href.startswith("http") else base_url + href
            # Use parent element text as body instead of fetching individual pages
            parent_text = a.find_parent().get_text(separator=" ", strip=True) if a.find_parent() else title
            results.append((label, title, full_url, datetime.now(timezone.utc), parent_text))
    except Exception as e:
        print(f"  ✗ Error: {e}")
    return results

# ─── SOURCE DEFINITIONS ───────────────────────────────────

def run_all_sources():
    tasks = [
        lambda: fetch_rss("David Carlin / Substack", "https://davidcarlin.substack.com/feed"),
        lambda: fetch_rss("Carbon Brief / DeBriefed", "https://www.carbonbrief.org/feed/",
                          filter_fn=lambda e: "debriefed" in e.get("link", "").lower() or
                          any("debriefed" in t.get("term", "").lower() for t in e.get("tags", []))),
        lambda: fetch_rss("Carbon Pulse", "https://carbon-pulse.com/feed"),
        lambda: fetch_scrape("SBTi News", "https://sciencebasedtargets.org/news",
                             "a[href*='/news/']", "https://sciencebasedtargets.org"),
        lambda: fetch_scrape("WRI News", "https://www.wri.org/news",
                             "a[href*='/news/']", "https://www.wri.org"),
        lambda: fetch_scrape("GHG Protocol", "https://ghgprotocol.org/blog",
                             "a[href*='/blog/']", "https://ghgprotocol.org"),
        lambda: fetch_scrape("Carbon Tracker", "https://carbontracker.org/reports/",
                             "a[href*='/reports/']", "https://carbontracker.org", timeout=30),
        lambda: fetch_scrape("PCAF", "https://carbonaccountingfinancials.com/news",
                             "a[href*='/news']", "https://carbonaccountingfinancials.com"),
        lambda: fetch_climate_adapt(),
    ]

    all_results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(t): t for t in tasks}
        for future in as_completed(futures):
            try:
                all_results.extend(future.result())
            except Exception as e:
                print(f"  ✗ Task error: {e}")
    return all_results

def fetch_climate_adapt():
    """EEA needs longer timeout — handled separately."""
    label = "Climate-ADAPT (EEA)"
    base  = "https://climate-adapt.eea.europa.eu"
    print(f"\n→ {label}")
    results = []
    try:
        r    = requests.get(f"{base}/en/news-archive", headers=HEADERS, timeout=45)
        soup = BeautifulSoup(r.text, "html.parser")
        seen = set()
        for a in soup.select("a[href*='/news-archive/']"):
            title = a.get_text(strip=True)
            href  = a.get("href", "")
            if not title or len(title) < 15 or href in seen:
                continue
            seen.add(href)
            url         = href if href.startswith("http") else base + href
            parent_text = a.find_parent().get_text(separator=" ", strip=True) if a.find_parent() else title
            results.append((label, title, url, datetime.now(timezone.utc), parent_text))
    except requests.exceptions.Timeout:
        print("  ⚠ Climate-ADAPT timed out — skipping.")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    return results

# ─── MAIN ─────────────────────────────────────────────────

def run():
    print("=" * 60)
    print(f"ESG Digest Scraper — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    all_articles = run_all_sources()
    print(f"\nCollected {len(all_articles)} candidate articles.")
    save_all_articles(all_articles)
    print("\n✅ Done.")

if __name__ == "__main__":
    run()
