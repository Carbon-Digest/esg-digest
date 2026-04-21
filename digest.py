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

    # Check if digest already exists for this week
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT id FROM digests WHERE week_number = %s", (TARGET_WEEK,))
    if cur.fetchone():
        print(f"Digest already exists for week {TARGET_WEEK} — skipping.")
        cur.close(); conn.close()
        return [], TARGET_WEEK, TARGET_YEAR
    cur.close(); conn.close()

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT source_label, title, url, body_text, published_at
        FROM articles
        WHERE week_number = %s AND processed = FALSE
    """, (TARGET_WEEK,))
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
        "ESG reporting standards latest",
        "carbon markets CBAM update",
        "climate finance developments",
        "carbon accounting net zero",
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

    return f"""You are an AI system that produces a weekly podcast called "The Climate Digest" — an automated, AI-generated briefing for expert practitioners in sustainability, climate finance, carbon accounting, and non-financial reporting.

Your audience are professionals who follow this space closely. Do not explain basic concepts. Do not provide background context unless it is directly relevant to understanding a new development. Assume the listener knows what CBAM, SBTi, GHG Protocol, ISSB, CSRD, and carbon markets are.

Your tone is: concise, factual, and direct. Focus exclusively on what is NEW this week — new publications, new regulations, new data, new positions. If something happened more than 7 days ago and is not directly relevant to a current development, do not include it.

TASK:
1. Read all the articles below — these are from the past 7 days only
2. Identify only the genuinely new developments — ignore anything that is background, historical context, or older than this week
3. Group related new developments into themes
4. Eliminate repetition — if multiple sources cover the same story, synthesize into one account
5. Use web search enrichment only to add the very latest context to current stories — not to add older background
6. Write a concise podcast script of approximately 2,500–3,500 words for an expert audience
7. do not include news or general information presented in previous episodes in detail.

SCRIPT STRUCTURE:
- [INTRO] Keep it short — 3 sentences maximum. Open with: "This is the Climate Digest, an AI-generated weekly briefing on climate change, climate finance, and non-financial reporting. Episode [N]. This week: [topic 1], [topic 2], and [topic 3]."
- [SECTIONS] One section per major theme. Transitions between sections should be a single smooth sentence — no abrupt stops. Vary transition phrases so they don't feel repetitive.
- [SOURCE MENTIONS] Attribute clearly but naturally within the flow of the sentence. Never start two consecutive sentences with a source name.
- [OUTRO] Two sentences maximum: "That concludes this week's Climate Digest, compiled automatically from [sources]. Links in the show notes."

WRITING STYLE:
- Sentences should vary in length — mix short punchy statements with longer explanatory ones
- Avoid repeating the same sentence structure back to back
- Never use filler phrases like "it is worth noting", "it is important to mention", "as we can see"
- Each paragraph should flow into the next with a clear logical connection

FORMATTING RULES:
- Write exactly as it will be spoken — no bullet points, no headers
- Mark meaningful pauses with [PAUSE]

OUTPUT: Return ONLY a JSON object with no markdown fences. The "script" field must use \\n\\n for paragraph breaks — never raw newlines or tab characters inside the JSON string:
{{
  "title": "The Climate Digest — Episode [N]",
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
        "temperature": 0.4,
        "response_format": {"type": "json_object"}
    }
    r = requests.post(MISTRAL_URL, headers=headers, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()

# ─── STEP 5: PARSE AND SAVE ───────────────────────────────

def save_digest(raw_response, week, year):
    clean = raw_response.strip()

    # Remove markdown fences if present
    clean = clean.replace("```json", "").replace("```", "").strip()

    # Try direct parse first
    try:
        digest = json.loads(clean)
    except json.JSONDecodeError:
        # Strip control characters and try again
        import re
        clean2 = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', clean)
        try:
            digest = json.loads(clean2)
        except json.JSONDecodeError:
            # Last resort: find the JSON object boundaries
            try:
                start = clean2.index('{')
                end   = clean2.rindex('}') + 1
                digest = json.loads(clean2[start:end])
            except Exception as e:
                print(f"All parse attempts failed: {e}")
                print(f"--- RAW RESPONSE (first 500 chars) ---")
                print(raw_response[:500])
                print(f"--------------------------------------")
                with open("digest_raw.txt", "w") as f:
                    f.write(raw_response)
                print("Full response saved to digest_raw.txt")
                return None

    # Get episode number (count existing digests + 1)
    conn_ep = get_conn()
    cur_ep  = conn_ep.cursor()
    cur_ep.execute("SELECT COUNT(*) FROM digests")
    ep_number = cur_ep.fetchone()[0] + 1
    cur_ep.close(); conn_ep.close()

    digest["title"] = f"The Climate Digest — Episode {ep_number}"

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

def mark_articles_processed(week):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE articles SET processed = TRUE
        WHERE week_number = %s
    """, (week,))
    conn.commit()
    cur.close(); conn.close()
    print("Articles marked as processed.")

# ─── STEP 7: CLEANUP OLD ARTICLES ────────────────────────

def cleanup_old_articles(week):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        DELETE FROM articles
        WHERE processed = TRUE
        AND week_number < %s
    """, (week,))
    deleted = cur.rowcount
    conn.commit()
    cur.close(); conn.close()
    print(f"Cleaned up {deleted} old articles.")

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
        mark_articles_processed(week)
        cleanup_old_articles(week)

if __name__ == "__main__":
    run()
