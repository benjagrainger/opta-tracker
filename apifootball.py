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
    # England
    "EPL": (39,  2025),  # Premier League
    "CHA": (40,  2025),  # Championship
    "LEO": (41,  2025),  # League One
    "LET": (42,  2025),  # League Two
    "FAC": (45,  2025),  # FA Cup
    "EFL": (48,  2025),  # Carabao Cup
    "WSL": (44,  2025),  # FA WSL
    # Spain
    "LL":  (140, 2025),  # LaLiga
    "LLT": (141, 2025),  # LaLiga 2 (Segunda)
    "CDR": (143, 2025),  # Copa del Rey
    # Germany
    "BUN": (78,  2025),  # Bundesliga
    "BU2": (79,  2025),  # Bundesliga 2
    "DFB": (81,  2025),  # DFB Pokal
    # France
    "LI1": (61,  2025),  # Ligue 1
    "LI2": (62,  2025),  # Ligue 2
    "CFR": (66,  2025),  # Coupe de France
    # Italy
    "SEA": (135, 2025),  # Serie A  ← Opta uses "SEA" not "SA"
    "SA":  (135, 2025),  # alias
    "SEB": (136, 2025),  # Serie B
    "CIT": (137, 2025),  # Coppa Italia
    # Scotland
    "SPL": (179, 2025),  # Scottish Premiership
    "SCU": (186, 2025),  # Scottish Cup
    # Netherlands
    "ERE": (88,  2025),  # Eredivisie
    # Portugal
    "PPG": (94,  2025),  # Primeira Liga
    "PRL": (94,  2025),  # alias
    # USA
    "MLS": (253, 2026),  # MLS
    "NWSL":(254, 2026),  # NWSL
    # Europe
    "UCL": (2,   2025),  # Champions League
    "UEL": (3,   2025),  # Europa League
    "ECL": (848, 2025),  # Conference League
    "UCF": (848, 2025),  # Conference League (código alternativo Opta)
    "UNL": (5,   2024),  # Nations League
    # Others
    "BRAS":(71,  2026),  # Brasileirao
    "BSA": (71,  2026),  # alias
    "LMX": (262, 2025),  # Liga MX
    "TUR": (203, 2025),  # Süper Lig
    "BEL": (144, 2025),  # Belgian Pro League
    "POR": (94,  2025),  # Portuguese Liga (alias)
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
    "AVL": "aston villa", "NOT": "nottingham",  "NFO": "nottingham",
    "MUN": "manchester united", "NEW": "newcastle",
    "WOL": "wolverhampton","FUL": "fulham",     "BRE": "brentford",
    "CRY": "crystal",     "IPS": "ipswich",     "SOU": "southampton",
    # LaLiga
    "VAL": "valencia",    "RAY": "rayo",        "RMA": "real madrid",
    "BAR": "barcelona",   "ATM": "atletico",    "SEV": "sevilla",
    "VIL": "villarreal",  "RSO": "sociedad",    "BET": "betis",
    "GIR": "girona",      "CEL": "celta",       "ALA": "alaves",
    "GET": "getafe",      "MLL": "mallorca",    "LEG": "leganes",
    "ESP": "espanyol",    "ATH": "athletic",    "OVI": "oviedo",
    # Ligue 1 — abreviaturas Opta reales
    "PSG": "paris",       "LIL": "lille",       "OL":  "lyon",
    "OLY": "lyon",        "ASM": "monaco",      "MON": "monaco",
    "NIC": "nice",        "STR": "strasbourg",  "NAN": "nantes",
    "REN": "rennes",      "OM":  "marseille",   "MRS": "marseille",
    "REI": "reims",       "TFC": "toulouse",    "TOU": "toulouse",
    "RCL": "lens",        "FCL": "lorient",     "HAC": "havre",
    "SCO": "angers",      "AJA": "auxerre",
    "BRE": "brest",       # NB: BRE=Brentford en EPL filtrado por liga
    # League One / League Two
    "STE": "stevenage",   "BRA": "bradford",    "BOL": "bolton",
    "CHF": "chester",     "SAL": "salford",     "GRI": "grimsby",
    "NOT": "notts county","CTN": "cheltenham",

    # Serie A
    "JUV": "juventus",    "INT": "inter",       "MIL": "milan",
    "ROM": "roma",        "LAZ": "lazio",       "NAP": "napoli",
    "ATL": "atalanta",    "FIO": "fiorentina",
    # Scottish PL
    "CEL": "celtic",      "RAN": "rangers",     "HEA": "hearts",
    "HIB": "hibernian",   "ABE": "aberdeen",
    # Serie A
    "JUV": "juventus",    "INT": "inter",       "MIL": "milan",
    "ROM": "roma",        "LAZ": "lazio",       "NAP": "napoli",
    "ATA": "atalanta",    "FIO": "fiorentina",  "TOR": "torino",
    "BOL": "bologna",     "UDI": "udinese",     "GEN": "genoa",
    "PAR": "parma",       "CAG": "cagliari",    "COM": "como",
    "VER": "verona",      "LEC": "lecce",       "EMP": "empoli",
    "SAS": "sassuolo",    "MOZ": "monza",       "VEN": "venezia",
    "CRE": "cremonese",   "PIS": "pisa",
    # Eredivisie
    "AJX": "ajax",        "PSV": "psv",         "FEY": "feyenoord",
    "AZA": "alkmaar",     "UTR": "utrecht",     "TWE": "twente",
    # Primeira Liga
    "BEN": "benfica",     "POR": "porto",       "SPO": "sporting",
    "BRA": "braga",       "GUI": "guimaraes",
    # Brasileirao
    "FLA": "flamengo",    "PAL": "palmeiras",   "SAO": "sao paulo",
    "COR": "corinthians", "GRE": "gremio",      "INT": "internacional",
    "FLU": "fluminense",  "ATH": "athletico",
    # Champions/Europa
    "BAY": "bayern",      "PSG": "paris",       "MCI": "manchester city",
    "REA": "real madrid", "BAR": "barcelona",
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


def _normalize(s: str) -> str:
    """Normalize umlauts and accents for robust matching."""
    return (s.lower()
            .replace("ü", "u").replace("ö", "o").replace("ä", "a")
            .replace("ñ", "n").replace("é", "e").replace("á", "a")
            .replace("í", "i").replace("ó", "o").replace("ú", "u")
            .replace("è", "e").replace("ê", "e").replace("â", "a"))


def _similarity(abbr: str, full: str) -> float:
    """Compare Opta abbreviation to API Football team name."""
    a, b = abbr.upper(), _normalize(full)
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


def find_fixture(fixtures: list, home: str, away: str, expected_date: str = None):
    """
    Fuzzy-match Opta abbreviations to an API Football fixture.
    Both teams must individually score > 0.45.
    If expected_date is given, rejects fixtures whose date differs by more than 1 day.
    """
    best, best_score = None, 0.0
    for f in fixtures:
        # Date sanity check
        if expected_date:
            fixture_date = f.get("fixture", {}).get("date", "")[:10]
            if fixture_date:
                try:
                    from datetime import datetime as _dt
                    delta = abs((_dt.strptime(fixture_date, "%Y-%m-%d") -
                                 _dt.strptime(expected_date, "%Y-%m-%d")).days)
                    if delta > 1:
                        continue
                except ValueError:
                    pass

        h_name = f.get("teams", {}).get("home", {}).get("name", "")
        a_name = f.get("teams", {}).get("away", {}).get("name", "")
        h_sim = _similarity(home, h_name)
        a_sim = _similarity(away, a_name)
        if h_sim < 0.45 or a_sim < 0.45:
            continue  # both teams must match
        score = (h_sim + a_sim) / 2
        if score > best_score:
            best_score = score
            best = f
    return best if best_score > 0.55 else None


def get_odds(fixture_id: int):
    """
    Fetch 1X2 pre-match odds from API Football for a given fixture.
    Tries all bookmakers until it finds one with Home/Draw/Away values.
    Returns {odds_home, odds_draw, odds_away} or None.
    """
    time.sleep(0.3)
    data = _get(f"{BASE_URL}/odds?fixture={fixture_id}&bet=1")  # bet=1 = Match Winner
    for entry in data.get("response", []):
        for bookmaker in entry.get("bookmakers", []):
            for bet in bookmaker.get("bets", []):
                if bet.get("id") == 1:  # Match Winner / 1X2
                    odds = {}
                    for v in bet.get("values", []):
                        name = v.get("value", "").lower()
                        try:
                            val = float(v.get("odd", 0))
                        except (ValueError, TypeError):
                            continue
                        if val <= 1.0:
                            continue  # invalid odd
                        if name == "home":
                            odds["odds_home"] = val
                        elif name == "draw":
                            odds["odds_draw"] = val
                        elif name == "away":
                            odds["odds_away"] = val
                    if len(odds) == 3:
                        return odds
    return None


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
