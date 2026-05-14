#!/usr/bin/env python3
"""
Opta Tracker — orquestador principal.

Uso:
  python run.py scrape      → captura ticker + cuotas de hoy
  python run.py results     → actualiza resultados de partidos jugados
  python run.py report      → imprime métricas (requiere datos acumulados)
  python run.py all         → scrape + results + report
"""
import sys
import time
from datetime import datetime, timezone

from db import init_db, get_conn
from opta import scrape_ticker, parse_match_date
from sofascore import compute_implied, add_deltas
from apifootball import (
    COMP_TO_LEAGUE,
    get_fixtures_for_date,
    find_fixture,
    get_odds as af_get_odds,
    get_result as af_get_result,
)
from analyze import print_report
from report import generate as generate_html

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def scrape():
    print(f"\n[{NOW}] Scraping Opta ticker...")
    matches = scrape_ticker()
    print(f"  → {len(matches)} partidos futuros encontrados")

    # Cache API Football fixtures per (comp, date) to minimise API calls
    af_cache = {}

    saved = skipped = no_odds = 0
    with get_conn() as conn:
        for m in matches:
            date_str, time_str = parse_match_date(m["dt_raw"])
            if not date_str:
                continue

            # Upsert prediction
            existing = conn.execute(
                "SELECT id, apifootball_id FROM predictions WHERE match_date=? AND home=? AND away=?",
                (date_str, m["home"], m["away"])
            ).fetchone()

            if existing:
                pred_id = existing["id"]
                af_id = existing["apifootball_id"]
                skipped += 1
            else:
                af_id = None
                cur = conn.execute(
                    """INSERT INTO predictions
                       (scraped_at, match_date, match_time_utc, comp, home, away,
                        prob_home, prob_draw, prob_away)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (NOW, date_str, time_str, m["comp"], m["home"], m["away"],
                     m["prob_home"], m["prob_draw"], m["prob_away"])
                )
                pred_id = cur.lastrowid
                saved += 1

            # Resolve API Football fixture ID (look up if not yet stored)
            if not af_id:
                comp_key = (m["comp"], date_str)
                if comp_key not in af_cache:
                    league_info = COMP_TO_LEAGUE.get(m["comp"])
                    if league_info:
                        lid, season = league_info
                        time.sleep(0.3)
                        af_cache[comp_key] = get_fixtures_for_date(lid, season, date_str)
                    else:
                        af_cache[comp_key] = []

                fixtures = af_cache.get(comp_key, [])
                fixture = find_fixture(fixtures, m["home"], m["away"])
                if fixture:
                    af_id = fixture["fixture"]["id"]
                    conn.execute(
                        "UPDATE predictions SET apifootball_id=? WHERE id=?",
                        (af_id, pred_id)
                    )

            # Fetch and store current odds from API Football
            if af_id:
                raw = af_get_odds(af_id)
                if raw:
                    d = compute_implied(raw)
                    d = add_deltas(d, m["prob_home"], m["prob_draw"], m["prob_away"])
                    conn.execute(
                        """INSERT INTO odds
                           (prediction_id, fetched_at, odds_home, odds_draw, odds_away,
                            impl_home, impl_draw, impl_away,
                            delta_home, delta_draw, delta_away)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (pred_id, NOW,
                         d["odds_home"], d["odds_draw"], d["odds_away"],
                         d["impl_home"], d["impl_draw"], d["impl_away"],
                         d["delta_home"], d["delta_draw"], d["delta_away"])
                    )
                else:
                    no_odds += 1

    print(f"  Nuevos: {saved} | Ya existían: {skipped} | Sin cuotas: {no_odds}")

    # Print PEV bets to console
    _print_pev_bets()

    # Always regenerate the HTML dashboard
    generate_html()


def update_results():
    print(f"\n[{NOW}] Actualizando resultados...")
    with get_conn() as conn:
        pending = conn.execute("""
            SELECT p.id, p.home, p.away, p.match_date, p.comp
            FROM predictions p
            WHERE p.id NOT IN (SELECT prediction_id FROM results)
              AND p.match_date <= date('now', '+1 day')
        """).fetchall()

        updated = 0
        for row in pending:
            res = af_get_result(row["home"], row["away"], row["comp"], row["match_date"])
            if res:
                conn.execute(
                    """INSERT OR REPLACE INTO results
                       (prediction_id, home_score, away_score, outcome, updated_at)
                       VALUES (?,?,?,?,?)""",
                    (row["id"], res["home_score"], res["away_score"], res["outcome"], NOW)
                )
                print(f"  ✓ {row['home']} vs {row['away']} ({row['match_date']}) → "
                      f"{res['home_score']}-{res['away_score']} [{res['outcome']}]")
                updated += 1

    print(f"  {updated} resultado(s) nuevos guardados")


def _print_pev_bets():
    """Print current positive-EV bets to console."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT p.comp, p.home, p.away, p.match_date,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away
            FROM predictions p
            JOIN odds o ON o.prediction_id = p.id
            WHERE o.id IN (
                SELECT MAX(id) FROM odds GROUP BY prediction_id
            )
              AND p.id NOT IN (SELECT prediction_id FROM results)
            ORDER BY p.match_date, p.home
        """).fetchall()

    candidates = []
    for r in rows:
        for side, opta, odds in [
            ("L", r["prob_home"], r["odds_home"]),
            ("E", r["prob_draw"], r["odds_draw"]),
            ("V", r["prob_away"], r["odds_away"]),
        ]:
            if opta and odds:
                ev = (opta / 100) * odds - 1
                if ev > 0:
                    team = r["home"] if side == "L" else (r["away"] if side == "V" else "Empate")
                    candidates.append({
                        "partido": f"{r['home']} vs {r['away']}",
                        "comp": r["comp"],
                        "fecha": r["match_date"],
                        "lado": side,
                        "equipo": team,
                        "opta_pct": opta,
                        "cuota": odds,
                        "ev": ev,
                    })

    if not candidates:
        print("\n  (No hay apuestas con PEV en el ticker actual)")
        return

    candidates.sort(key=lambda x: x["ev"], reverse=True)
    print(f"\n  APUESTAS CON PEV:")
    print(f"  {'Partido':<22} {'Liga':5} {'Lado':2} {'Opta%':>6} {'Cuota':>6} {'EV':>7}")
    print("  " + "-" * 58)
    for c in candidates[:15]:
        print(f"  {c['partido']:<22} {c['comp']:5} {c['lado']:2} "
              f"{c['opta_pct']:>6.1f} {c['cuota']:>6.2f} {c['ev']:>+7.1%}")


if __name__ == "__main__":
    init_db()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"

    if cmd in ("scrape", "all"):
        scrape()
    if cmd in ("results", "all"):
        update_results()
    if cmd in ("report", "all"):
        print_report()
