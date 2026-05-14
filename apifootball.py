"""Fetch match results from API Football (v3.football.api-sports.io)."""
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta
from difflib import SequenceMatcher

API_KEY = os.environ.get("API_FOOTBALL_KEY", "118876a460deec78fb27b410a6e1ad65")
BASE_URL = "https://v3.football.api-sports.io"

# (league_id, season) per Opta competition code
COMP_TO_LEAGUE = {
    "LL":  (140, 2025),  # LaLiga
    "EPL": (39,  2025),  # Premier League
    "BUN": (78,  2025),  # Bundesliga
    "LI1": (61,  2025),  # Ligue 1
    "SA":  (135, 2025),  # Serie A
    "MLS": (253, 2026),  # MLS
    "WSL": (44,  2025),  # FA WSL
    "LEO": (41,  2025),  # League One
    "LET": (42,  2025),  # League Two
    "CHA": (40,  2025),  # Championship
    "FAC": (45,  2025),  # FA Cup
    "SPL": (179, 2025),  # Scottish Premiership
}

# Opta abbreviation → keyword in API Football team name (lowercase)
OPTA_ABBREV = {
    # Bundesliga
    "SGE": "frankfurt",   "BMG": "gladbach",    "FCB": "bayern",
    "KOE": "koln",        "STP": "pauli",       "SVW": "werder",
    "FCU": "union",       "SCF": "freiburg",    "RBL": "leipzig",
    "HDH": "heidenheim",  "LEV": "leverkusen",  "VFB": "stuttgart",
    "BVB": "dortmund",    "M05": "mainz",       "FCA": "augsburg",
    "WOB": "wolfsburg",   "TSG": "hoffenheim",  "HSV": "hamburg",
    # MLS
    "SKC": "kansas",      "LAG": "galaxy",      "LAF": "angeles fc",
    "STL": "st. louis",   "ATL": "atlanta",     "NSH": "nashville",
    "CLB": "columbus",    "ATX": "austin",      "HOU": "houston",
    "SDG": "san diego",   "RSL": "salt lake",   "SJ":  "san jose",
    "VAN": "vancouver",   "POR": "portland",    "SEA": "seattle",
    "CLT": "charlotte",   "NYC": "new york city","NYR": "red bulls",
    "PHI": "philadelphia","DC":  "dc united",   "CHI": "chicago",
    "ORL": "orlando",     "NE":  "new england", "MTL": "montreal",
    "DAL": "dallas",      "MIN": "minnesota",   "COL": "colorado",
    "TOR": "toronto",     "CIN": "cincinnati",  "MIA": "miami",
    # WSL
    "LFC": "liverpool",   "ARS": "arsenal",     "MCI": "manchester city",
    "MNU": "manchester united", "EVE": "everton","LEI": "leicester",
    "BHA": "brighton",    "LCL": "london city", "WHU": "west ham",
    "AST": "aston villa", "TOT": "tottenham",   "CHE": "chelsea",
    # EPL
    "AVL": "aston villa", "NOT": "nottingham",  "NEW": "newcastle",
    "WOL": "wolverhampton","FUL": "fulham",     "BRE": "brentford",
    "CRY": "crystal",     "IPS": "ipswich",     "SOU": "southampton",
    # LaLiga
    "VAL": "valencia",    "RAY": "rayo",        "RMA": "real madrid",
    "BAR": "barcelona",   "ATM": "atletico",    "SEV": "sevilla",
    "VIL": "villarreal",  "RSO": "sociedad",    "BET": "betis",
    "GIR": "girona",      "CEL": "celta",       "ALA": "alaves",
    "GET": "getafe",      "MLL": "mallorca",    "LEG": "leganes",
    "ESP": "espanyol",    "ATH": "athletic",    "OVI": "oviedo",
    # Ligue 1
    "PSG": "paris",       "LIL": "lille",       "OLY": "lyon",
    "MON": "monaco",      "NIC": "nice",        "STR": "strasbourg",
    "NAN": "nantes",      "REN": "rennes",      "MRS": "marseille",
    "REI": "reims",       "TOU": "toulouse",    "RCL": "lens",
    # League One / League Two
    "STE": "stevenage",   "BRA": "bradford",    "BOL": "bolton",
    "CHF": "cheltenham",  "SAL": "salford",     "GRI": "grimsby",
    # Serie A
    "JUV": "juventus",    "INT": "inter",       "MIL": "milan",
    "ROM": "roma",        "LAZ": "lazio",       "NAP": "napoli",
    "ATL": "atalanta",    "FIO": "fiorentina",
    # Scottish PL
    "CEL": "celtic",      "RAN": "rangers",     "HEA": "hearts",
    "HIB": "hibernian",   "ABE": "aberdeen",
}


def _get(url: str, retries: int = 3) -> dict:
    """GET with retries and exponential backoff. Returns {} on persistent failure."""
    headers = {
        "x-apisports-key": API_KEY,
        "Accept": "application/json",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except Exception as e:
            if attempt < retries - 1:
                wait = 5 * (2 ** attempt)
                print(f"    [apifootball] {e} — retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    [apifootball] Failed after {retries} attempts: {e}")
                return {}
    return {}


def _similarity(abbr: str, full: str) -> float:
    """Compare Opta abbreviation to API Football team name."""
    a, b = abbr.upper(), full.lower()
    base = SequenceMatcher(None, a.lower(), b).ratio()
    words = b.replace("-", " ").split()
    # Bonus: abbr is a prefix of any word
    if any(w.startswith(a.lower()) for w in words):
        base = max(base, 0.75)
    # Bonus: abbr matches word initials
    initials = "".join(w[0] for w in words if w)
    if a.lower() == initials:
        base = max(base, 0.80)
    # Bonus: known keyword appears in name
    keyword = OPTA_ABBREV.get(a)
    if keyword and keyword in b:
        base = max(base, 0.85)
    return base


def get_fixtures_for_date(league_id: int, season: int, date_str: str) -> list:
    """Returns list of fixture response dicts for a league on a given date."""
    data = _get(f"{BASE_URL}/fixtures?league={league_id}&season={season}&date={date_str}")
    return data.get("response", [])


def find_fixture(fixtures: list, home: str, away: str):
    """Fuzzy-match Opta abbreviations to an API Football fixture. Both teams must score > 0.4."""
    best, best_score = None, 0.0
    for f in fixtures:
        h_name = f.get("teams", {}).get("home", {}).get("name", "")
        a_name = f.get("teams", {}).get("away", {}).get("name", "")
        h_sim = _similarity(home, h_name)
        a_sim = _similarity(away, a_name)
        if h_sim < 0.4 or a_sim < 0.4:
            continue  # both teams must match
        score = (h_sim + a_sim) / 2
        if score > best_score:
            best_score = score
            best = f
    return best if best_score > 0.5 else None


def get_result(home: str, away: str, comp: str, match_date: str):
    """
    Search API Football for the result of home vs away in the given competition on match_date.
    Also checks match_date+1 to handle western hemisphere games that run past midnight UTC.
    Returns {home_score, away_score, outcome} or None if not found / not finished.
    outcome: 'H' / 'D' / 'A'
    """
    league_info = COMP_TO_LEAGUE.get(comp)
    if not league_info:
        return None
    league_id, season = league_info

    dates_to_check = [match_date]
    try:
        dt = datetime.strptime(match_date, "%Y-%m-%d")
        dates_to_check.append((dt + timedelta(days=1)).strftime("%Y-%m-%d"))
    except Exception:
        pass

    for date_str in dates_to_check:
        time.sleep(0.3)  # be gentle with the API
        fixtures = get_fixtures_for_date(league_id, season, date_str)
        if not fixtures:
            continue
        fixture = find_fixture(fixtures, home, away)
        if not fixture:
            continue

        status = fixture.get("fixture", {}).get("status", {}).get("short", "")
        if status not in ("FT", "AET", "PEN"):
            return None  # found the match but it's not finished yet

        goals = fixture.get("goals", {})
        hs = goals.get("home")
        as_ = goals.get("away")
        if hs is None or as_ is None:
            return None

        outcome = "H" if hs > as_ else ("A" if hs < as_ else "D")
        h_name = fixture.get("teams", {}).get("home", {}).get("name", "?")
        a_name = fixture.get("teams", {}).get("away", {}).get("name", "?")
        print(f"    [apifootball] {home} vs {away} → matched '{h_name}' vs '{a_name}' [{hs}-{as_}]")
        return {"home_score": hs, "away_score": as_, "outcome": outcome}

    return None
