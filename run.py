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
import json
from datetime import datetime, timedelta, timezone

from db import init_db, get_conn
from opta import scrape_ticker, parse_match_date
from sofascore import (
    get_events_for_date, find_event, get_odds,
    compute_implied, add_deltas, get_result,
)
from analyze import print_report
from report import generate as generate_html

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def scrape():
    print(f"\n[{NOW}] Scraping Opta ticker...")
    matches = scrape_ticker()
    print(f"  → {len(matches)} partidos futuros encontrados")

    # Cache Sofascore events per date to avoid repeated API calls
    sf_cache: dict[str, list] = {}

    saved = skipped = no_odds = 0
    with get_conn() as conn:
        for m in matches:
            date_str, time_str = parse_match_date(m["dt_raw"])
            if not date_str:
                continue

            # Upsert prediction
            existing = conn.execute(
                "SELECT id, sofascore_id FROM predictions WHERE match_date=? AND home=? AND away=?",
                (date_str, m["home"], m["away"])
            ).fetchone()

            if existing:
                pred_id = existing["id"]
                sf_id = existing["sofascore_id"]
                skipped += 1
            else:
                # Find Sofascore event
                if date_str not in sf_cache:
                    sf_cache[date_str] = get_events_for_date(date_str)
                event = find_event(sf_cache[date_str], m["home"], m["away"], m["comp"])
                sf_id = event["id"] if event else None

                cur = conn.execute(
                    """INSERT INTO predictions
                       (scraped_at, match_date, match_time_utc, comp, home, away,
                        prob_home, prob_draw, prob_away, sofascore_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (NOW, date_str, time_str, m["comp"], m["home"], m["away"],
                     m["prob_home"], m["prob_draw"], m["prob_away"], sf_id)
                )
                pred_id = cur.lastrowid
                saved += 1

            # Fetch and store current odds
            if sf_id:
                raw = get_odds(sf_id)
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

    # Print top value bets
    _print_top_value()

    # Always regenerate the HTML dashboard
    generate_html()


def update_results():
    print(f"\n[{NOW}] Actualizando resultados...")
    with get_conn() as conn:
        pending = conn.execute("""
            SELECT p.id, p.sofascore_id, p.home, p.away, p.match_date
            FROM predictions p
            WHERE p.sofascore_id IS NOT NULL
              AND p.id NOT IN (SELECT prediction_id FROM results)
              AND p.match_date <= date('now', '+1 day')
        """).fetchall()

        updated = 0
        for row in pending:
            res = get_result(row["sofascore_id"])
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


def _print_top_value():
    """Print today's top value bets from the DB."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT p.comp, p.home, p.away, p.match_date,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away,
                   o.delta_home, o.delta_draw, o.delta_away
            FROM predictions p
            JOIN odds o ON o.prediction_id = p.id
            WHERE o.id IN (
                SELECT MAX(id) FROM odds GROUP BY prediction_id
            )
            ORDER BY o.fetched_at DESC
        """).fetchall()

    candidates = []
    for r in rows:
        for side, opta, odds, delta in [
            ("L", r["prob_home"], r["odds_home"], r["delta_home"]),
            ("E", r["prob_draw"], r["odds_draw"], r["delta_draw"]),
            ("V", r["prob_away"], r["odds_away"], r["delta_away"]),
        ]:
            if delta and delta >= 3.0:
                team = r["home"] if side == "L" else (r["away"] if side == "V" else "Empate")
                ev = round((opta / 100) * odds - 1, 3)
                candidates.append({
                    "partido": f"{r['home']} vs {r['away']}",
                    "comp": r["comp"],
                    "fecha": r["match_date"],
                    "lado": side,
                    "equipo": team,
                    "opta_pct": opta,
                    "cuota": odds,
                    "delta": delta,
                    "ev": ev,
                })

    if not candidates:
        print("\n  (No hay bets con delta ≥ 3pp en el ticker actual)")
        return

    candidates.sort(key=lambda x: x["delta"], reverse=True)
    print(f"\n  TOP VALUE BETS (delta ≥ 3pp):")
    print(f"  {'Partido':<20} {'Liga':5} {'Lado':2} {'Opta%':>6} {'Cuota':>6} {'Δ':>6} {'EV':>7}")
    print("  " + "-" * 60)
    for c in candidates[:10]:
        print(f"  {c['partido']:<20} {c['comp']:5} {c['lado']:2} "
              f"{c['opta_pct']:>6.1f} {c['cuota']:>6.2f} {c['delta']:>+6.1f} {c['ev']:>+7.1%}")


if __name__ == "__main__":
    init_db()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"

    if cmd in ("scrape", "all"):
        scrape()
    if cmd in ("results", "all"):
        update_results()
    if cmd in ("report", "all"):
        print_report()
