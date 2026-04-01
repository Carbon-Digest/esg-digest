import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import os
import psycopg2  # Use psycopg2 for PostgreSQL

# ─── CONFIG ───────────────────────────────────────────────

NEON_URL = os.environ["NEON_POSTGRES_URL"]  # e.g., "postgres://user:pass@ep-cool-123456.../dbname"
ONE_WEEK_AGO = datetime.now(timezone.utc) - timedelta(days=7)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ESG-Digest-Bot/1.0)"}

# ─── NEON HELPER ────────────────────────────────────────

def save_article(source_label, title, url, published_at, body_text):
    """Insert article into Neon, skip if URL already exists."""
    try:
        conn = psycopg2.connect(NEON_URL)
        cursor = conn.cursor()

        # Check if URL exists
        cursor.execute("SELECT id FROM articles WHERE url = %s", (url,))
        if cursor.fetchone():
            print(f"  ~ Skipping (exists): {title[:60]}")
            return

        # Insert new article
        now = datetime.now(timezone.utc)
        cursor.execute("""
            INSERT INTO articles (
                source_label, title, url, published_at, body_text, processed, week_number,
