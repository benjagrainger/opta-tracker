"""Scrape the Opta Match Ticker from theanalyst.com using Playwright."""
import re
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

TICKER_URL = "https://dataviz.theanalyst.com/opta-football-predictions/?ticker=true"

def scrape_ticker() -> list:
    """Return list of upcoming matches with Opta probabilities."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(TICKER_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_selector("[class*='match-card_']", timeout=15000)

        cards = page.query_selector_all("[class*='match-card_']")
        matches = []
        for card in cards:
            text = card.inner_text().replace("\n", "|").strip()
            if "%" not in text:
                continue
            parts = text.split("|")
            parts = [p.strip() for p in parts if p.strip()]
            # Expected: COMP | DATE @ TIME | HOME | PCT | AWAY | PCT | DRAW | PCT
            try:
                comp = parts[0]
                dt_str = parts[1]          # e.g. "May 12 @ 01:00 PM"
                home = parts[2]
                prob_home = float(parts[3].rstrip("%"))
                away = parts[4]
                prob_away = float(parts[5].rstrip("%"))
                # draw label + pct
                prob_draw = float(parts[7].rstrip("%"))
                matches.append({
                    "comp": comp,
                    "dt_raw": dt_str,
                    "home": home,
                    "away": away,
                    "prob_home": prob_home,
                    "prob_draw": prob_draw,
                    "prob_away": prob_away,
                })
            except (IndexError, ValueError):
                continue

        browser.close()
    return matches


def parse_match_date(dt_raw: str, year: int = None) -> tuple[str, str]:
    """
    'May 12 @ 01:00 PM' → ('2026-05-12', '13:00')
    Times in the ticker appear to be US Eastern.
    Returns (date_str, time_str) in local ticker time.
    """
    if year is None:
        year = datetime.now().year
    try:
        clean = dt_raw.replace("@", "").strip()
        dt = datetime.strptime(f"{clean} {year}", "%b %d  %I:%M %p %Y")
        return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")
    except ValueError:
        return "", dt_raw


if __name__ == "__main__":
    matches = scrape_ticker()
    print(f"Found {len(matches)} upcoming matches\n")
    for m in matches:
        d, t = parse_match_date(m["dt_raw"])
        print(f"{m['comp']:5} {d} {t}  {m['home']:4} {m['prob_home']:5.1f}% "
              f"| E {m['prob_draw']:5.1f}% | {m['away']:4} {m['prob_away']:5.1f}%")
