import os
import json
import requests
from datetime import datetime, timezone
from supabase import create_client

# ─── CONFIG ───────────────────────────────────────────────

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]
MISTRAL_KEY   = os.environ["MISTRAL_API_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

MISTRAL_MODEL = "mistral-small-latest"  # free tier model
MISTRAL_URL   = "https://api.mistral.ai/v1/chat/completions"

# ─── STEP 1: FETCH THIS WEEK'S ARTICLES ───────────────────

def fetch_weeks_articles():
    now = datetime.now(timezone.utc)
    iso = now.isocalendar()
    week, year = iso[1], iso[0]

    print(f"Fetching articles for week {week}/{year}...")

    result = supabase.table("articles") \
        .select("source_label, title, url, body_text, published_at") \
        .eq("week_number", week) \
        .eq("year", year) \
        .eq("processed", False) \
        .execute()

    articles = result.data
    print(f"Found {len(articles)} articles.")
    return articles, week, year

# ─── STEP 2: WEB SEARCH FOR ENRICHMENT ───────────────────
# Uses DuckDuckGo — no API key needed

def web_search(query):
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": query, "format": "json", "no_html": 1, "skip_disambig": 1},
            timeout=10
        )
        data = r.json()
        results = []
        # Abstract text
        if data.get("Abstract"):
            results.append(data["Abstract"])
        # Related topics
        for topic in data.get("RelatedTopics", [])[:3]:
            if isinstance(topic, dict) and topic.get("Text"):
                results.append(topic["Text"])
        return " | ".join(results) if results else ""
    except Exception as e:
        print(f"  Search error: {e}")
        return ""

def enrich_with_search(themes):
    print("Enriching themes with web search...")
    enrichments = {}
    search_queries = [
        "ESG reporting standards 2026 latest news",
        "carbon markets CBAM update 2026",
        "climate finance developments March 2026",
        "sustainability disclosure regulations 2026",
    ]
    for q in search_queries:
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
Content: {a['body_text'][:3000]}
"""

    enrichment_text = ""
    for query, result in enrichments.items():
        if result:
            enrichment_text += f"\nSearch: {query}\nResult: {result}\n"

    prompt = f"""You are an AI system that produces a weekly podcast called "The ESG & Climate Briefing" — an automated, AI-generated digest for sustainability, climate finance, and non-financial reporting practitioners.

Your tone is: factual, clear, and informative. You are not a human host — you are an AI summarising and synthesising information from trusted sources. You do not express opinions, use casual language, or pretend to have personal perspectives. You present information accurately and attribute it clearly to its sources.

TASK:
1. Read all the articles below from this week's sources
2. Identify key themes, grouping related stories together
3. Eliminate repetition — if multiple sources cover the same story, synthesize them into one richer account noting differences in framing
4. Weave in the web search enrichment results naturally where relevant
5. Write a complete, natural podcast script of approximately 3,500–4,500 words (shorter if it's a quiet week)

SCRIPT STRUCTURE:
- [INTRO] Always open with exactly this format, filling in the blanks:
  "This is the ESG and Climate Briefing — an AI-generated digest of last week's most important developments in sustainability, climate finance, carbon accounting, and non-financial reporting. This is week [X] of [YEAR]. This week's digest covers [topic 1], [topic 2], and [topic 3]."
- [SECTIONS] One section per major theme, with clear factual transitions between them
- [SOURCE MENTIONS] Always attribute information to its source clearly and directly. For example: "The Science Based Targets initiative published...", "Carbon Brief reported...", "The GHG Protocol announced...", "The World Resources Institute noted...", "The European Environment Agency's Climate-ADAPT platform recorded...". Vary the phrasing but keep it factual and direct.
- [OUTRO] Close with: "That concludes this week's ESG and Climate Briefing. This digest was compiled by an AI system from the following sources: [list the sources used this week]. Source links are available in the show notes. This briefing is generated automatically each week."

FORMATTING RULES:
- Write exactly as it will be spoken — no bullet points, no headers
- Tone is factual, clear and informative — avoid overly casual language, rhetorical questions, or personal opinions
- Do not use phrases like "I think", "in my view", or anything implying human perspective
- Mark meaningful pauses with [PAUSE]
- If a week is quiet on a topic, state it plainly and move on

OUTPUT: Return ONLY a JSON object with no markdown fences:
{{
  "title": "The ESG & Climate Briefing — Week [X], [YEAR]",
  "summary": "2-3 sentence plain text summary for show notes",
  "themes": ["theme 1", "theme 2", "theme 3"],
  "script": "the full podcast script as a single string with paragraph breaks as \\n\\n"
}}

THIS WEEK'S ARTICLES:
{articles_text}

LATEST WEB SEARCH ENRICHMENT:
{enrichment_text if enrichment_text else "No additional search results available."}
"""
    return prompt

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
        print("Raw response saved to digest_raw.txt for inspection")
        return None

    supabase.table("digests").insert({
        "week_number": week,
        "year": year,
        "title": digest.get("title"),
        "summary": digest.get("summary"),
        "themes": digest.get("themes"),
        "script": digest.get("script"),
        "audio_url": None,
        "created_at": datetime.now(timezone.utc).isoformat()
    }).execute()

    print(f"\n✅ Digest saved: {digest.get('title')}")
    print(f"Summary: {digest.get('summary')}")
    print(f"Themes: {', '.join(digest.get('themes', []))}")
    print(f"Script length: {len(digest.get('script', ''))} characters")

    return digest

# ─── STEP 6: MARK ARTICLES AS PROCESSED ───────────────────

def mark_articles_processed(week, year):
    supabase.table("articles") \
        .update({"processed": True}) \
        .eq("week_number", week) \
        .eq("year", year) \
        .execute()
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

    enrichments = enrich_with_search(articles)
    prompt = build_prompt(articles, enrichments)
    raw_response = generate_digest(prompt)
    digest = save_digest(raw_response, week, year)

    if digest:
        mark_articles_processed(week, year)

if __name__ == "__main__":
    run()
