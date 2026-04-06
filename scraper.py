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

def get_conn():
    return psycopg2.connect(NEON_URL)

def save_article(source_label, title, url, published_at, body_text):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT id FROM articles WHERE url = %s", (url,))
        if cur.fetchone():
            print(f"  ~ Skipping (exists): {title[:60]}")
            cur.close(); conn.close()
            return
        cur.execute("""
            SELECT COUNT(*) FROM articles
            WHERE source_label = %s AND week_number = %s
        """, (source_label, TARGET_WEEK))
        if cur.fetchone()[0] >= MAX_ARTICLES_PER_SOURCE:
            print(f"  ~ Limit reached for {source_label}")
            cur.close(); conn.close()
            return
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
        try:
            cur.close(); conn.close()
        except Exception:
            pass

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

def fetch_scrape(label, url, link_selector, base_url):
    """Generic page scraper — uses title + excerpt from listing page only, no individual fetches."""
    print(f"\n→ {label}")
    results = []
    try:
        r    = requests.get(url, headers=HEADERS, timeout=20)
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
                             "a[href*='/reports/']", "https://carbontracker.org"),
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
    print(f"\nSaving {len(all_articles)} candidate articles...")

    for args in all_articles:
        save_article(*args)

    print("\n✅ Done.")

if __name__ == "__main__":
    run()
