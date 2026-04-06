import os
import json
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL      = os.environ["NEON_POSTGRES_URL"]
MISTRAL_KEY   = os.environ["MISTRAL_API_KEY"]
MISTRAL_MODEL = "mistral-small-latest"
MISTRAL_URL   = "https://api.mistral.ai/v1/chat/completions"

_now        = datetime.now(timezone.utc)
_iso        = _now.isocalendar()
TARGET_WEEK = int(os.environ.get("TARGET_WEEK", _iso[1]))
TARGET_YEAR = int(os.environ.get("TARGET_YEAR", _iso[0]))

def get_conn():
    return psycopg2.connect(NEON_URL)

# ─── STEP 1: FETCH ARTICLES ───────────────────────────────

def fetch_weeks_articles():
    print(f"Fetching articles for week {TARGET_WEEK}/{TARGET_YEAR}...")
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT source_label, title, url, body_text, published_at
        FROM articles
        WHERE week_number = %s AND year = %s AND processed = FALSE
    """, (TARGET_WEEK, TARGET_YEAR))
    articles = cur.fetchall()
    cur.close(); conn.close()
    print(f"Found {len(articles)} articles.")
    return list(articles)

# ─── STEP 2: WEB SEARCH ENRICHMENT ───────────────────────

def web_search(query):
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": query, "format": "json", "no_html": 1, "skip_disambig": 1},
            timeout=10
        )
        data = r.json()
        results = []
        if data.get("Abstract"):
            results.append(data["Abstract"])
        for topic in data.get("RelatedTopics", [])[:3]:
            if isinstance(topic, dict) and topic.get("Text"):
                results.append(topic["Text"])
        return " | ".join(results)
    except Exception as e:
        print(f"  Search error: {e}")
        return ""

def enrich_with_search():
    print("Enriching with web search...")
    queries = [
        "ESG reporting standards 2026 latest",
        "carbon markets CBAM update 2026",
        "climate finance developments 2026",
        "sustainability disclosure regulations 2026",
    ]
    return {q: web_search(q) for q in queries}

# ─── STEP 3: BUILD PROMPT ─────────────────────────────────

def build_prompt(articles, enrichments):
    articles_text = ""
    for i, a in enumerate(articles, 1):
        articles_text += f"""
---
Article {i}
Source: {a['source_label']}
Title: {a['title']}
Content: {str(a['body_text'])[:3000]}
"""
    enrichment_text = "\n".join(
        f"Search: {q}\nResult: {r}" for q, r in enrichments.items() if r
    )

    return f"""You are an AI system that produces a weekly podcast called "The ESG and Climate Briefing" — an automated, AI-generated digest for sustainability, climate finance, and non-financial reporting practitioners.

Your tone is: factual, clear, and informative. You are not a human host — you are an AI summarising and synthesising information from trusted sources. You do not express opinions, use casual language, or pretend to have personal perspectives.

TASK:
1. Read all the articles below from this week's sources
2. Identify key themes, grouping related stories together
3. Eliminate repetition — synthesize overlapping stories into one richer account
4. Weave in the web search enrichment results naturally where relevant
5. Write a complete podcast script of approximately 3,500–4,500 words (shorter if quiet week)

SCRIPT STRUCTURE:
- [INTRO] Always open with: "This is the ESG and Climate Briefing — an AI-generated digest of last week's most important developments in sustainability, climate finance, carbon accounting, and non-financial reporting. This is week [X] of [YEAR]. This week's digest covers [topic 1], [topic 2], and [topic 3]."
- [SECTIONS] One section per major theme, with clear factual transitions
- [SOURCE MENTIONS] Always attribute clearly: "The Science Based Targets initiative published
