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
    get_fixtures_for_date_any,
    find_fixture,
    get_odds as af_get_odds,
    get_fixture_status,
)
from analyze import print_report
from report import generate as generate_html

_now_dt = datetime.now(timezone.utc)
NOW = _now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
IS_BET_SNAPSHOT = 1 if _now_dt.hour == 23 else 0  # 23:00 UTC = 8pm Chile


def scrape():
    print(f"\n[{NOW}] Scraping Opta ticker...")
    matches = scrape_ticker()
    print(f"  → {len(matches)} partidos futuros encontrados")

    # Cache API Football fixtures per (comp, date) to minimise API calls
    af_cache = {}
    # Fallback cache: todos los fixtures por fecha (para comps desconocidas)
    af_date_cache = {}

    saved = skipped = no_odds = 0
    with get_conn() as conn:
        for m in matches:
            date_str, time_str = parse_match_date(m["dt_raw"])
            if not date_str:
                continue

            # Upsert prediction
            existing = conn.execute(
                "SELECT id, apifootball_id, prob_home, prob_draw, prob_away "
                "FROM predictions WHERE match_date=? AND home=? AND away=?",
                (date_str, m["home"], m["away"])
            ).fetchone()

            if existing:
                pred_id = existing["id"]
                af_id = existing["apifootball_id"]
                skipped += 1
                # Record history only when probs actually changed
                if (existing["prob_home"] != m["prob_home"] or
                        existing["prob_draw"] != m["prob_draw"] or
                        existing["prob_away"] != m["prob_away"]):
                    conn.execute(
                        "INSERT INTO prob_history (prediction_id, scraped_at, prob_home, prob_draw, prob_away) "
                        "VALUES (?,?,?,?,?)",
                        (pred_id, NOW, m["prob_home"], m["prob_draw"], m["prob_away"])
                    )
                # Always update to latest
                conn.execute(
                    "UPDATE predictions SET prob_home=?, prob_draw=?, prob_away=? WHERE id=?",
                    (m["prob_home"], m["prob_draw"], m["prob_away"], pred_id)
                )
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
                # Record initial probabilities
                conn.execute(
                    "INSERT INTO prob_history (prediction_id, scraped_at, prob_home, prob_draw, prob_away) "
                    "VALUES (?,?,?,?,?)",
                    (pred_id, NOW, m["prob_home"], m["prob_draw"], m["prob_away"])
                )

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
                        # Competición no mapeada: buscar en todos los fixtures del día
                        if date_str not in af_date_cache:
                            time.sleep(0.3)
                            af_date_cache[date_str] = get_fixtures_for_date_any(date_str)
                            print(f"  [!] Comp '{m['comp']}' sin mapeo — fallback por fecha ({date_str}): "
                                  f"{len(af_date_cache[date_str])} fixtures encontrados")
                        af_cache[comp_key] = af_date_cache[date_str]

                fixtures = af_cache.get(comp_key, [])
                fixture = find_fixture(fixtures, m["home"], m["away"], expected_date=date_str)
                if fixture:
                    af_id = fixture["fixture"]["id"]
                    h_name = fixture["teams"]["home"]["name"]
                    a_name = fixture["teams"]["away"]["name"]
                    lg_name = fixture.get("league", {}).get("name", "")
                    # True UTC kick-off from API Football (overrides Opta Eastern time)
                    af_date = fixture.get("fixture", {}).get("date", "")
                    af_time_utc = af_date[11:16] if len(af_date) >= 16 else None
                    conn.execute(
                        "UPDATE predictions SET apifootball_id=?, home_name=?, away_name=?, league_name=?"
                        + (", match_time_utc=?" if af_time_utc else "") + " WHERE id=?",
                        (af_id, h_name, a_name, lg_name) + ((af_time_utc,) if af_time_utc else ()) + (pred_id,)
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
                            delta_home, delta_draw, delta_away, is_bet_snapshot)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (pred_id, NOW,
                         d["odds_home"], d["odds_draw"], d["odds_away"],
                         d["impl_home"], d["impl_draw"], d["impl_away"],
                         d["delta_home"], d["delta_draw"], d["delta_away"],
                         IS_BET_SNAPSHOT)
                    )
                else:
                    no_odds += 1

    print(f"  Nuevos: {saved} | Ya existían: {skipped} | Sin cuotas: {no_odds}")

    # Print PEV bets to console
    _print_pev_bets()

    # Always regenerate the HTML dashboard
    generate_html()


def _resolve_apifootball_id(conn, row):
    """Busca y almacena el apifootball_id para predicciones que aún no lo tienen.
    Retorna el ID encontrado o None."""
    date_str = row["match_date"]
    league_info = COMP_TO_LEAGUE.get(row["comp"])

    if league_info:
        lid, season = league_info
        time.sleep(0.3)
        fixtures = get_fixtures_for_date(lid, season, date_str)
    else:
        time.sleep(0.3)
        fixtures = get_fixtures_for_date_any(date_str)
        if fixtures:
            print(f"    [!] Comp '{row['comp']}' sin mapeo — fallback por fecha: {len(fixtures)} fixtures")

    fixture = find_fixture(fixtures, row["home"], row["away"], expected_date=date_str)
    if not fixture:
        return None

    af_id = fixture["fixture"]["id"]
    h_name = fixture["teams"]["home"]["name"]
    a_name = fixture["teams"]["away"]["name"]
    lg_name = fixture.get("league", {}).get("name", "")
    af_date = fixture.get("fixture", {}).get("date", "")
    af_time_utc = af_date[11:16] if len(af_date) >= 16 else None

    conn.execute(
        "UPDATE predictions SET apifootball_id=?, home_name=?, away_name=?, league_name=?"
        + (", match_time_utc=?" if af_time_utc else "") + " WHERE id=?",
        (af_id, h_name, a_name, lg_name) + ((af_time_utc,) if af_time_utc else ()) + (row["id"],)
    )
    print(f"  → Fixture resuelto: {row['home']} vs {row['away']} → apifootball_id={af_id}")
    return af_id


def update_results():
    print(f"\n[{NOW}] Actualizando resultados y marcadores en vivo...")
    with get_conn() as conn:
        # Partidos sin resultado: los que ya deberían haber comenzado (+ 1 día de margen para MLS etc.)
        pending = conn.execute("""
            SELECT p.id, p.home, p.away, p.match_date, p.match_time_utc, p.comp, p.apifootball_id
            FROM predictions p
            WHERE p.id NOT IN (SELECT prediction_id FROM results)
              AND p.match_date <= date('now', '+1 day')
            ORDER BY p.match_date, p.match_time_utc
        """).fetchall()

        updated = live_updated = 0
        for row in pending:
            af_id = row["apifootball_id"]

            # Si no tenemos el ID de API Football, intentar resolverlo automáticamente
            if not af_id:
                af_id = _resolve_apifootball_id(conn, row)

            if not af_id:
                continue

            time.sleep(0.3)
            status_data = get_fixture_status(af_id)
            if not status_data or not status_data.get("status"):
                continue

            status = status_data["status"]
            hs     = status_data.get("home_score")
            as_    = status_data.get("away_score")
            elapsed = status_data.get("elapsed")

            if status in ("FT", "AET", "PEN"):
                # Partido terminado → guardar resultado y limpiar live_scores
                if hs is not None and as_ is not None:
                    outcome = "H" if hs > as_ else ("A" if hs < as_ else "D")
                    conn.execute(
                        """INSERT OR REPLACE INTO results
                           (prediction_id, home_score, away_score, outcome, updated_at)
                           VALUES (?,?,?,?,?)""",
                        (row["id"], hs, as_, outcome, NOW)
                    )
                    conn.execute("DELETE FROM live_scores WHERE prediction_id=?", (row["id"],))
                    print(f"  ✓ {row['home']} vs {row['away']} → {hs}-{as_} [{outcome}]")
                    updated += 1

            elif status in ("1H", "HT", "2H", "ET", "BT", "P", "SUSP", "INT", "LIVE"):
                # En juego → actualizar live_scores
                conn.execute(
                    """INSERT OR REPLACE INTO live_scores
                       (prediction_id, status, home_score, away_score, elapsed, updated_at)
                       VALUES (?,?,?,?,?,?)""",
                    (row["id"], status, hs if hs is not None else 0,
                     as_ if as_ is not None else 0, elapsed, NOW)
                )
                live_updated += 1
            # NS (no iniciado) o desconocido → no hacer nada aún

    print(f"  {updated} resultado(s) nuevos | {live_updated} partido(s) en vivo actualizados")


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


def backfill_names():
    """
    Fill home_name/away_name/league_name/match_time_utc (true UTC from API Football)
    for all predictions that have an apifootball_id.
    Uses GET /fixtures?id={id} — one call per prediction.
    """
    from apifootball import _get, BASE_URL
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, apifootball_id FROM predictions
            WHERE apifootball_id IS NOT NULL
        """).fetchall()

    if not rows:
        print("  backfill_names: nada que actualizar")
        return

    print(f"  backfill_names: actualizando {len(rows)} predicciones...")
    updated = 0
    with get_conn() as conn:
        for r in rows:
            time.sleep(0.3)
            data = _get(f"{BASE_URL}/fixtures?id={r['apifootball_id']}")
            fixtures = data.get("response", [])
            if not fixtures:
                continue
            f = fixtures[0]
            h_name  = f["teams"]["home"]["name"]
            a_name  = f["teams"]["away"]["name"]
            lg_name = f.get("league", {}).get("name", "")
            # True UTC kick-off time (overrides Eastern time stored by Opta scraper)
            af_date = f.get("fixture", {}).get("date", "")
            af_time_utc = af_date[11:16] if len(af_date) >= 16 else None
            conn.execute(
                "UPDATE predictions SET home_name=?, away_name=?, league_name=?"
                + (", match_time_utc=?" if af_time_utc else "") + " WHERE id=?",
                (h_name, a_name, lg_name) + ((af_time_utc,) if af_time_utc else ()) + (r["id"],)
            )
            updated += 1
    print(f"  backfill_names: {updated} registros actualizados")


def refresh_odds():
    """Actualiza cuotas solo para partidos que comienzan en las próximas 6 horas.
    Se llama desde el job de resultados (cada 5 min) para tener odds frescas
    cerca del kick-off, donde el mercado se mueve más.
    Nunca marca is_bet_snapshot=1 (eso solo lo hace el job de scrape a las 23:00 UTC)."""
    print(f"\n[{NOW}] Actualizando cuotas (partidos próximas 6h)...")
    with get_conn() as conn:
        upcoming = conn.execute("""
            SELECT p.id, p.apifootball_id, p.prob_home, p.prob_draw, p.prob_away
            FROM predictions p
            WHERE p.id NOT IN (SELECT prediction_id FROM results)
              AND p.id NOT IN (SELECT prediction_id FROM live_scores)
              AND p.apifootball_id IS NOT NULL
              AND (p.match_date || ' ' || COALESCE(p.match_time_utc,'23:59'))
                  BETWEEN datetime('now') AND datetime('now', '+6 hours')
        """).fetchall()

        if not upcoming:
            print("  Sin partidos próximos en las siguientes 6 horas.")
            return

        updated = 0
        for row in upcoming:
            raw = af_get_odds(row["apifootball_id"])
            if not raw:
                continue
            d = compute_implied(raw)
            d = add_deltas(d, row["prob_home"], row["prob_draw"], row["prob_away"])
            conn.execute(
                """INSERT INTO odds
                   (prediction_id, fetched_at, odds_home, odds_draw, odds_away,
                    impl_home, impl_draw, impl_away,
                    delta_home, delta_draw, delta_away, is_bet_snapshot)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,0)""",
                (row["id"], NOW,
                 d["odds_home"], d["odds_draw"], d["odds_away"],
                 d["impl_home"], d["impl_draw"], d["impl_away"],
                 d["delta_home"], d["delta_draw"], d["delta_away"])
            )
            updated += 1

    print(f"  {updated} cuota(s) actualizadas")


if __name__ == "__main__":
    init_db()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"

    if cmd in ("scrape", "all"):
        scrape()
    if cmd in ("results", "all"):
        update_results()
    if cmd == "refresh_odds":
        refresh_odds()
        generate_html()
    if cmd in ("report", "all"):
        print_report()
    if cmd == "backfill_names":
        backfill_names()
        generate_html()
