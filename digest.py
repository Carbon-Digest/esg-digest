import os
import json
from datetime import datetime, timezone
from supabase import create_client
import google.generativeai as genai

# ─── CONFIG ───────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GEMINI_KEY   = os.environ["GEMINI_API_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_KEY)

model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    tools="google_search_retrieval"
)

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

# ─── STEP 2: BUILD PROMPT ─────────────────────────────────

def build_prompt(articles):
    articles_text = ""
    for i, a in enumerate(articles, 1):
        articles_text += f"""
---
Article {i}
Source: {a['source_label']}
Title: {a['title']}
URL: {a['url']}
Content: {a['body_text'][:3000]}
"""

    prompt = f"""You are the host of a weekly podcast called "The ESG & Climate Briefing" — a professional, intelligent digest for sustainability, climate finance, and non-financial reporting practitioners.

Your style is: informed, analytical, and conversational — like a trusted expert colleague briefing a senior professional during their commute. Not a cheerleader — you present developments clearly, note tensions between sources, and offer your own analytical framing where useful.

TASK:
1. Read all the articles below from this week's sources
2. Identify key themes, grouping related stories together
3. Eliminate repetition — if multiple sources cover the same story, synthesize them into one richer account noting differences in framing
4. Use your Google Search grounding to enrich each major theme with the very latest developments not yet in the articles
5. Write a complete, natural podcast script of approximately 3,500–4,500 words (shorter if it's a quiet week)

SCRIPT STRUCTURE:
- [INTRO] Warm but professional opening. State the week and tease 2-3 headline topics.
- [SECTIONS] One section per major theme, with natural transitions between them
- [OUTRO] Brief closing that looks ahead to what to watch next week

FORMATTING RULES:
- Write exactly as it will be spoken — no bullet points, no headers
- Use natural spoken language: contractions, rhetorical questions, short sentences for emphasis
- Mark meaningful pauses with [PAUSE]
- Reference organisations naturally: "the Science Based Targets initiative announced..." not "Source: SBTi"
- If a week is quiet on a topic, say so briefly and move on

OUTPUT: Return ONLY a JSON object with no markdown fences:
{{
  "title": "The ESG & Climate Briefing — Week [X], [YEAR]",
  "summary": "2-3 sentence plain text summary for show notes",
  "themes": ["theme 1", "theme 2", "theme 3"],
  "script": "the full podcast script as a single string with paragraph breaks as \\n\\n"
}}

HERE ARE THIS WEEK'S ARTICLES:
{articles_text}
"""
    return prompt

# ─── STEP 3: CALL GEMINI ──────────────────────────────────

def generate_digest(prompt):
    print("Sending to Gemini Flash (with Google Search grounding)...")

    response = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(
            max_output_tokens=8000,
            temperature=0.4
        )
    )

    return response.text.strip()

# ─── STEP 4: PARSE AND SAVE ───────────────────────────────

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

# ─── STEP 5: MARK ARTICLES AS PROCESSED ───────────────────

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

    prompt = build_prompt(articles)
    raw_response = generate_digest(prompt)
    digest = save_digest(raw_response, week, year)

    if digest:
        mark_articles_processed(week, year)

if __name__ == "__main__":
    run()
