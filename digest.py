import os
import json
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL    = os.environ["NEON_POSTGRES_URL"]
MISTRAL_KEY = os.environ["MISTRAL_API_KEY"]

_now        = datetime.now(timezone.utc)
_iso        = _now.isocalendar()
TARGET_WEEK = int(os.environ.get("TARGET_WEEK", _iso[1]))
TARGET_YEAR = int(os.environ.get("TARGET_YEAR", _iso[0]))

MISTRAL_MODEL = "mistral-small-latest"
MISTRAL_URL   = "https://api.mistral.ai/v1/chat/completions"

def get_conn():
    return psycopg2.connect(NEON_URL)

# ─── STEP 1: FETCH THIS WEEK'S ARTICLES ───────────────────

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
    return list(articles), TARGET_WEEK, TARGET_YEAR

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
    enrichments = {}
    for q in queries:
        result = web_search(q)
        if result:
            enrichments[q] = result
    return enrichments

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
- [SOURCE MENTIONS] Always attribute clearly: "The Science Based Targets initiative published...", "Carbon Brief reported...", "The GHG Protocol announced...", etc.
- [OUTRO] Close with: "That concludes this week's ESG and Climate Briefing. This digest was compiled by an AI system from the following sources: [list sources used]. Source links are available in the show notes. This briefing is generated automatically each week."

FORMATTING RULES:
- Write exactly as it will be spoken — no bullet points, no headers
- Factual, clear, informative tone — no casual language or personal opinions
- Mark meaningful pauses with [PAUSE]

OUTPUT: Return ONLY a JSON object with no markdown fences:
{{
  "title": "The ESG and Climate Briefing — Week [X], [YEAR]",
  "summary": "2-3 sentence plain text summary for show notes",
  "themes": ["theme 1", "theme 2", "theme 3"],
  "script": "the full podcast script as a single string with paragraph breaks as \\n\\n"
}}

THIS WEEK'S ARTICLES:
{articles_text}

LATEST WEB SEARCH ENRICHMENT:
{enrichment_text if enrichment_text else "No additional search results available."}
"""

# ─── STEP 4: CALL MISTRAL ─────────────────────────────────

def generate_digest(prompt):
    print(f"Sending to Mistral ({MISTRAL_MODEL})...")
    headers = {
        "Authorization": f"Bearer {MISTRAL_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": MISTRAL_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 8000,
        "temperature": 0.4
    }
    r = requests.post(MISTRAL_URL, headers=headers, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()

# ─── STEP 5: PARSE AND SAVE ───────────────────────────────

def save_digest(raw_response, week, year):
    clean = raw_response.replace("```json", "").replace("```", "").strip()
    try:
        digest = json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        with open("digest_raw.txt", "w") as f:
            f.write(raw_response)
        print("Raw response saved to digest_raw.txt")
        return None

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO digests
            (week_number, year, title, summary, themes, script, audio_url, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, NULL, %s)
    """, (
        week, year,
        digest.get("title"),
        digest.get("summary"),
        json.dumps(digest.get("themes", [])),
        digest.get("script"),
        datetime.now(timezone.utc).isoformat()
    ))
    conn.commit()
    cur.close(); conn.close()

    print(f"\n✅ Digest saved: {digest.get('title')}")
    print(f"Themes: {', '.join(digest.get('themes', []))}")
    print(f"Script length: {len(digest.get('script', ''))} chars")
    return digest

# ─── STEP 6: MARK ARTICLES PROCESSED ─────────────────────

def mark_articles_processed(week, year):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE articles SET processed = TRUE
        WHERE week_number = %s AND year = %s
    """, (week, year))
    conn.commit()
    cur.close(); conn.close()
    print("Articles marked as processed.")

# ─── MAIN ─────────────────────────────────────────────────

def run():
    print("=" * 60)
    print(f"ESG Digest Generator — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    articles, week, year = fetch_weeks_articles()
    if not articles:
        print("No new articles this week. Exiting.")
        return
    enrichments = enrich_with_search()
    prompt      = build_prompt(articles, enrichments)
    raw         = generate_digest(prompt)
    digest      = save_digest(raw, week, year)
    if digest:
        mark_articles_processed(week, year)

if __name__ == "__main__":
    run()
