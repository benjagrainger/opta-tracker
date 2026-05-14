"""Fetch odds and results from Sofascore API."""
import json
import time
import urllib.request
from datetime import datetime, timedelta
from difflib import SequenceMatcher

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.sofascore.com/",
    "Cache-Control": "no-cache",
}

def _get(url: str, retries: int = 3) -> dict:
    """GET with retries and exponential backoff. Returns {} on persistent failure."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except Exception as e:
            if attempt < retries - 1:
                wait = 5 * (2 ** attempt)  # 5s, 10s, 20s
                print(f"    [sofascore] {e} — retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    [sofascore] Failed after {retries} attempts: {e}")
                return {}
    return {}


def get_events_for_date(date_str: str) -> list:
    """date_str: 'YYYY-MM-DD'. Returns list of Sofascore event dicts (empty on error)."""
    data = _get(f"https://api.sofascore.com/api/v1/sport/football/scheduled-events/{date_str}")
    return data.get("events", [])


# Opta abbreviation → keyword that appears in the Sofascore team name (lowercase)
OPTA_ABBREV = {
    # Bundesliga
    "SGE": "frankfurt", "BMG": "gladbach", "FCB": "bayern", "KOE": "köln",
    "STP": "pauli",     "SVW": "werder",   "FCU": "union",  "SCF": "freiburg",
    "RBL": "leipzig",   "HDH": "heidenheim","LEV": "leverkusen","VFB": "stuttgart",
    "BVB": "dortmund",  "M05": "mainz",    "FCA": "augsburg","WOB": "wolfsburg",
    "TSG": "hoffenheim","HSV": "hamburg",
    # MLS
    "SKC": "kansas",    "LAG": "galaxy",   "LAF": "los angeles fc",
    "STL": "st. louis", "ATL": "atlanta",  "NSH": "nashville","CLB": "columbus",
    "ATX": "austin",    "HOU": "houston",  "SDG": "san diego",
    "RSL": "salt lake", "SJ":  "san jose", "VAN": "vancouver",
    "POR": "portland",  "SEA": "seattle",  "CLT": "charlotte",
    "NYC": "new york city", "NYR": "red bulls", "PHI": "philadelphia",
    "DC":  "dc united", "CHI": "chicago",  "ORL": "orlando",
    "NE":  "new england","MTL": "montreal", "DAL": "dallas",
    "MIN": "minnesota", "COL": "colorado", "TOR": "toronto",
    "CIN": "cincinnati","MIA": "miami",    "ATL": "atlanta",
    # WSL
    "LFC": "liverpool", "ARS": "arsenal",  "CHE": "chelsea",
    "MCI": "manchester city", "MNU": "manchester united",
    "EVE": "everton",   "LEI": "leicester","TOT": "tottenham",
    "BHA": "brighton",  "LCL": "london city","WHU": "west ham",
    "AST": "aston villa",
    # LaLiga
    "VAL": "valencia",  "RAY": "rayo",     "RMA": "real madrid",
    "BAR": "barcelona", "ATM": "atlético", "SEV": "sevilla",
    "VIL": "villarreal","RSO": "sociedad", "BET": "betis",
    "GIR": "girona",    "CEL": "celta",    "ALA": "alavés",
    "GET": "getafe",    "MLL": "mallorca", "LEG": "leganés",
    "VCF": "valencia",  "ESP": "espanyol", "ATH": "athletic",
    "OVI": "oviedo",
}


def find_event(events: list, home: str, away: str, comp: str = None) -> dict:
    """Fuzzy-match home/away team abbreviations to a Sofascore event.
    Raises the threshold and optionally filters by Opta competition code."""

    # Sofascore uniqueTournament IDs for leagues Opta tracks
    COMP_TO_TOURNAMENT = {
        "LL":   8,     # LaLiga
        "EPL":  17,    # Premier League
        "BUN":  35,    # Bundesliga
        "LI1":  34,    # Ligue 1
        "SA":   23,    # Serie A
        "MLS":  242,   # MLS
        "CHA":  18,    # Championship
        "SPL":  36,    # Scottish Premiership
        "WSL":  1044,  # Women's Super League
        "FAC":  19,    # FA Cup
    }

    def similarity(abbr: str, full: str) -> float:
        """Compare Opta abbreviation to Sofascore team name."""
        a, b = abbr.upper(), full.lower()
        base = SequenceMatcher(None, a.lower(), b).ratio()
        words = b.replace("-", " ").split()
        # Bonus: abbr is a prefix of any word (e.g. CIN → Cincinnati)
        if any(w.startswith(a.lower()) for w in words):
            base = max(base, 0.75)
        # Bonus: abbr matches initials (e.g. LAG → LA Galaxy)
        initials = "".join(w[0] for w in words if w)
        if a.lower() == initials:
            base = max(base, 0.80)
        # Bonus: known abbreviation keyword appears in the name
        keyword = OPTA_ABBREV.get(a)
        if keyword and keyword in b:
            base = max(base, 0.85)
        return base

    target_tournament = COMP_TO_TOURNAMENT.get(comp) if comp else None

    best, best_score = None, 0.0
    for e in events:
        # If we know the tournament, skip events from other leagues
        if target_tournament:
            tid = e.get("tournament", {}).get("uniqueTournament", {}).get("id")
            if tid and tid != target_tournament:
                continue

        # Use full name for keyword matching, shortName as fallback for sequence similarity
        h_short = e.get("homeTeam", {}).get("shortName", "")
        h_full  = e.get("homeTeam", {}).get("name", "")
        a_short = e.get("awayTeam", {}).get("shortName", "")
        a_full  = e.get("awayTeam", {}).get("name", "")
        h = h_full or h_short
        a = a_full or a_short
        score = (similarity(home, h) + similarity(away, a)) / 2
        if score > best_score:
            best_score = score
            best = e

    return best if best_score > 0.5 else None


def get_odds(event_id: int) -> dict:
    """
    Returns {odds_home, odds_draw, odds_away} or None.
    Only accepts 1X2 full-time markets (3 choices: Home / Draw / Away).
    """
    try:
        time.sleep(0.5)  # be gentle
        data = _get(f"https://api.sofascore.com/api/v1/event/{event_id}/odds/1/featured")
        featured = data.get("featured", {}).get("default")
        if not featured:
            return None

        # Validate it's a 1X2 market
        market_name = featured.get("marketName", "").lower()
        valid_markets = ("full time", "1x2", "match result", "result", "winner", "match winner")
        if not any(m in market_name for m in valid_markets):
            return None

        choices = featured.get("choices", [])
        if len(choices) != 3:
            return None

        def frac2dec(s: str) -> float:
            n, d = s.split("/")
            return round(int(n) / int(d) + 1, 3)

        # Map by choice name (Home/1, Draw/X, Away/2) instead of assuming order
        odds = {}
        for c in choices:
            name = c.get("name", "").strip().lower()
            frac = c.get("fractionalValue") or c.get("initialFractionalValue")
            if not frac:
                continue
            val = frac2dec(frac)
            if name in ("1", "home", "home team", "1 (home)"):
                odds["odds_home"] = val
            elif name in ("x", "draw", "tie"):
                odds["odds_draw"] = val
            elif name in ("2", "away", "away team", "2 (away)"):
                odds["odds_away"] = val

        # Fallback to positional if names didn't match
        if len(odds) < 3:
            odds = {
                "odds_home": frac2dec(choices[0]["fractionalValue"]),
                "odds_draw": frac2dec(choices[1]["fractionalValue"]),
                "odds_away": frac2dec(choices[2]["fractionalValue"]),
            }

        return odds if len(odds) == 3 else None
    except Exception:
        return None


def compute_implied(odds: dict) -> dict:
    """Add normalised implied probabilities to an odds dict."""
    rH = 1 / odds["odds_home"]
    rX = 1 / odds["odds_draw"]
    rA = 1 / odds["odds_away"]
    tot = rH + rX + rA
    return {
        **odds,
        "impl_home": round(rH / tot * 100, 2),
        "impl_draw": round(rX / tot * 100, 2),
        "impl_away": round(rA / tot * 100, 2),
    }


def add_deltas(d: dict, prob_home: float, prob_draw: float, prob_away: float) -> dict:
    """Add Δ = Opta% - impl% to the odds+implied dict."""
    return {
        **d,
        "delta_home": round(prob_home - d["impl_home"], 2),
        "delta_draw": round(prob_draw - d["impl_draw"], 2),
        "delta_away": round(prob_away - d["impl_away"], 2),
    }


def get_result(event_id: int) -> dict:
    """
    Returns {home_score, away_score, outcome} or None if match not finished.
    outcome: 'H' / 'D' / 'A'
    """
    try:
        data = _get(f"https://api.sofascore.com/api/v1/event/{event_id}")
        event = data.get("event", {})
        status = event.get("status", {}).get("type", "")
        if status not in ("finished",):
            return None
        hs = event.get("homeScore", {}).get("current")
        as_ = event.get("awayScore", {}).get("current")
        if hs is None or as_ is None:
            return None
        if hs > as_:
            outcome = "H"
        elif hs < as_:
            outcome = "A"
        else:
            outcome = "D"
        return {"home_score": hs, "away_score": as_, "outcome": outcome}
    except Exception:
        return None
