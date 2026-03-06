import os
import json
from datetime import datetime, timezone
from supabase import create_client
import anthropic

# ─── CONFIG ───────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

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

# ─── STEP 2: BUILD PROMPT FOR CLAUDE ──────────────────────

def build_prompt(articles):
    # Format articles into a readable block for Claude
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

    system_prompt = """You are the producer and host of a weekly podcast called "The ESG & Climate Briefing" — a professional, intelligent digest for sustainability, climate finance, and non-financial reporting practitioners.

Your style is: informed, analytical, and conversational. You are not a cheerleader — you present developments clearly, note tensions or contradictions between sources, and briefly offer your own analytical framing where useful. Think of yourself as a trusted expert colleague briefing a senior professional during their commute.

Your job is to:
1. Read all the articles provided from this week's sources
2. Identify the key themes and developments, grouping related stories together
3. Eliminate repetition — if multiple sources cover the same story, synthesize them into one richer account noting any differences in framing
4. Use web search to enrich each major theme with the very latest developments not yet covered by the sources
5. Write a complete, natural podcast script of approximately 3,500-4,500 words (roughly 20 minutes when read aloud at a natural pace — shorter if it's a quiet week)

SCRIPT STRUCTURE:
- [INTRO] Warm but professional opening. State the week, tease 2-3 headline topics.
- [SECTION 1–N] One section per major theme. Each section has a clear title (spoken naturally, not announced like a heading). Transitions between sections should feel natural.
- [ENRICHMENT] For each major theme, weave in the freshest developments from your web search — do not create a separate "web search" section, integrate it naturally.
- [OUTRO] Brief closing that looks ahead to what to watch next week. Sign off warmly.

FORMATTING RULES:
- Write exactly as it will be spoken — no bullet points, no headers in the final script
- Use natural spoken language: contractions, rhetorical questions, short sentences for emphasis
- Mark pauses with [PAUSE] where useful for emphasis
- Never say "According to our sources" — just report the information naturally
- Do not mention article URLs or source names formally — you can say "the Science Based Targets initiative announced..." or "Carbon Brief reported..." but keep it natural
- If a week is quiet on a topic, say so briefly and move on

OUTPUT FORMAT:
Return a JSON object with exactly these fields:
{
  "title": "episode title (e.g. 'The ESG & Climate Briefing — Week 10, 2026')",
  "summary": "2-3 sentence plain text summary of the episode for show notes",
  "themes": ["list", "of", "main", "themes", "covered"],
  "script": "the full podcast script as a single string with natural paragraph breaks"
}
Return ONLY the JSON — no preamble, no markdown fences."""

    user_prompt = f"""Here are this week's articles from our monitored sources:

{articles_text}

Please use web search to find the very latest developments on the key themes before writing the script. Then write the full podcast script as instructed."""

    return system_prompt, user_prompt

# ─── STEP 3: CALL CLAUDE WITH WEB SEARCH ──────────────────

def generate_digest(system_prompt, user_prompt):
    print("Sending to Claude API (with web search)...")

    response = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=8000,
        system=system_prompt,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": user_prompt}]
    )

    # Extract the final text response (after any tool use)
    script_text = ""
    for block in response.content:
        if block.type == "text":
            script_text += block.text

    return script_text.strip()

# ─── STEP 4: PARSE AND SAVE TO SUPABASE ───────────────────

def save_digest(raw_response, week, year):
    # Clean up any accidental markdown fences
    clean = raw_response.replace("```json", "").replace("```", "").strip()

    try:
        digest = json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        print("Raw response saved to digest_raw.txt for inspection")
        with open("digest_raw.txt", "w") as f:
            f.write(raw_response)
        return None

    # Save to Supabase
    supabase.table("digests").insert({
        "week_number": week,
        "year": year,
        "title": digest.get("title"),
        "summary": digest.get("summary"),
        "themes": digest.get("themes"),
        "script": digest.get("script"),
        "audio_url": None,   # filled in Step 4 (TTS)
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
        print("No new articles found for this week. Exiting.")
        return

    system_prompt, user_prompt = build_prompt(articles)
    raw_response = generate_digest(system_prompt, user_prompt)
    digest = save_digest(raw_response, week, year)

    if digest:
        mark_articles_processed(week, year)

if __name__ == "__main__":
    run()
