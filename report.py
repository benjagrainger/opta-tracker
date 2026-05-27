"""Genera el dashboard HTML. Se llama automáticamente después de cada scrape."""
from datetime import datetime, timedelta
from pathlib import Path
from db import get_conn


def utc_to_chile_dt(time_str, date_str):
    """Convierte UTC (date_str YYYY-MM-DD, time_str HH:MM) → (chile_date, chile_time).
    Apr–Sep: CLT (UTC-4) · Oct–Mar: CLST (UTC-3)

    Maneja el desfase de fecha Opta/Eastern: match_date viene en hora local del ticker
    (Eastern ≈ Chile en verano), pero match_time_utc viene de API Football en UTC.
    Para partidos nocturnos de América (21:00–23:59 local = 01:00–03:59 UTC),
    el UTC ya cruzó la medianoche, por lo que la fecha UTC real es match_date + 1 día.
    Heurística segura: si h < 12 UTC, el partido es de la noche anterior en Opta/Eastern.
    """
    if not time_str or not date_str:
        return date_str or "", ""
    try:
        h, m = map(int, time_str.split(":"))
        base = datetime.strptime(date_str, "%Y-%m-%d")
        # Partidos nocturnos americanos: UTC h < 12 → base UTC es el día siguiente al Opta date
        if h < 12:
            base += timedelta(days=1)
        offset = 4 if 4 <= base.month <= 9 else 3
        dt_chile = base + timedelta(hours=h, minutes=m) - timedelta(hours=offset)
        return dt_chile.strftime("%Y-%m-%d"), dt_chile.strftime("%H:%M")
    except Exception:
        return date_str, time_str


def utc_to_chile(time_str, date_str=None):
    """Wrapper legado: retorna solo la hora Chile. Usar utc_to_chile_dt cuando se necesite la fecha."""
    _, t = utc_to_chile_dt(time_str, date_str or datetime.utcnow().strftime("%Y-%m-%d"))
    return t

OUTPUT         = Path(__file__).parent / "docs" / "index.html"
HISTORY_OUTPUT = Path(__file__).parent / "docs" / "history.html"


def load_data():
    with get_conn() as conn:
        # Current value bets: latest odds + first odds per prediction (for movement)
        value_bets = conn.execute("""
            SELECT p.comp, p.home, p.away,
                   COALESCE(p.home_name, p.home) AS home_display,
                   COALESCE(p.away_name, p.away) AS away_display,
                   COALESCE(p.league_name, p.comp) AS league_display,
                   p.match_date, p.match_time_utc,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away,
                   o.fetched_at,
                   of.odds_home AS first_odds_home,
                   of.odds_draw AS first_odds_draw,
                   of.odds_away AS first_odds_away
            FROM predictions p
            LEFT JOIN odds o ON o.id = (SELECT MAX(id) FROM odds WHERE prediction_id = p.id)
            LEFT JOIN odds of ON of.id = (SELECT MIN(id) FROM odds WHERE prediction_id = p.id)
            WHERE p.id NOT IN (SELECT prediction_id FROM results)
              AND (p.match_date || ' ' || COALESCE(p.match_time_utc, '23:59')) > datetime('now')
            ORDER BY p.match_date, p.match_time_utc, p.home
        """).fetchall()

        # Results: best available pre-match odds, prioritising day-before snapshot.
        # snapshot_type: 'official' (is_bet_snapshot=1 day before) |
        #                'approx'   (any 22-23h odds day before)    |
        #                'early'    (odds from >1 day before match)
        results = conn.execute("""
            SELECT p.comp, p.home, p.away,
                   COALESCE(p.home_name, p.home) AS home_display,
                   COALESCE(p.away_name, p.away) AS away_display,
                   COALESCE(p.league_name, p.comp) AS league_display,
                   p.match_date, p.match_time_utc,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away,
                   r.home_score, r.away_score, r.outcome,
                   CASE
                     WHEN o.is_bet_snapshot = 1
                          AND DATE(o.fetched_at) = DATE(p.match_date, '-1 day') THEN 'official'
                     WHEN DATE(o.fetched_at) = DATE(p.match_date, '-1 day')     THEN 'approx'
                     ELSE 'early'
                   END AS snapshot_type
            FROM predictions p
            JOIN results r ON r.prediction_id = p.id
            JOIN odds o ON o.id = (
                SELECT COALESCE(
                    (SELECT id FROM odds
                     WHERE prediction_id = p.id AND is_bet_snapshot = 1
                       AND DATE(fetched_at) < p.match_date
                     ORDER BY fetched_at DESC LIMIT 1),
                    (SELECT id FROM odds
                     WHERE prediction_id = p.id
                       AND strftime('%H', fetched_at) IN ('22','23')
                       AND DATE(fetched_at) < p.match_date
                     ORDER BY fetched_at DESC LIMIT 1),
                    (SELECT id FROM odds
                     WHERE prediction_id = p.id
                       AND DATE(fetched_at) < p.match_date
                     ORDER BY fetched_at DESC LIMIT 1)
                )
            )
            ORDER BY p.match_date DESC, COALESCE(p.match_time_utc,'23:59') DESC
            LIMIT 100
        """).fetchall()

        # Odds ~8am Chile día anterior (11:00 UTC)
        results_8am = conn.execute("""
            SELECT p.comp, p.home, p.away,
                   COALESCE(p.home_name, p.home) AS home_display,
                   COALESCE(p.away_name, p.away) AS away_display,
                   COALESCE(p.league_name, p.comp) AS league_display,
                   p.match_date, p.match_time_utc,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away,
                   r.outcome
            FROM predictions p
            JOIN results r ON r.prediction_id = p.id
            JOIN odds o ON o.id = (
                SELECT id FROM odds
                WHERE prediction_id = p.id
                  AND DATE(fetched_at) = DATE(p.match_date, '-1 day')
                  AND CAST(strftime('%H', fetched_at) AS INTEGER) BETWEEN 9 AND 13
                ORDER BY ABS(CAST(strftime('%H', fetched_at) AS INTEGER) - 11)
                LIMIT 1
            )
            ORDER BY p.match_date DESC LIMIT 100
        """).fetchall()

        # Últimas odds antes del kick-off
        results_kickoff = conn.execute("""
            SELECT p.comp, p.home, p.away,
                   COALESCE(p.home_name, p.home) AS home_display,
                   COALESCE(p.away_name, p.away) AS away_display,
                   COALESCE(p.league_name, p.comp) AS league_display,
                   p.match_date, p.match_time_utc,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away,
                   r.outcome
            FROM predictions p
            JOIN results r ON r.prediction_id = p.id
            JOIN odds o ON o.id = (
                SELECT id FROM odds
                WHERE prediction_id = p.id
                  AND strftime('%Y-%m-%d %H:%M', fetched_at) < p.match_date || ' ' || COALESCE(p.match_time_utc, '23:59')
                ORDER BY fetched_at DESC
                LIMIT 1
            )
            ORDER BY p.match_date DESC LIMIT 100
        """).fetchall()

        stats = conn.execute("""
            SELECT COUNT(*) as total FROM predictions
        """).fetchone()

        # Partidos en vivo o pendientes de confirmar.
        # Ventana: entre -5h (partido más largo posible) y +5min del inicio.
        try:
            live_matches = conn.execute("""
                SELECT p.comp, p.home, p.away,
                       COALESCE(p.home_name, p.home) AS home_display,
                       COALESCE(p.away_name, p.away) AS away_display,
                       COALESCE(p.league_name, p.comp) AS league_display,
                       p.match_date, p.match_time_utc,
                       p.prob_home, p.prob_draw, p.prob_away,
                       ob.odds_home AS bet_odds_home,
                       ob.odds_draw AS bet_odds_draw,
                       ob.odds_away AS bet_odds_away,
                       l.status, l.home_score, l.away_score, l.elapsed, l.updated_at
                FROM predictions p
                LEFT JOIN live_scores l ON l.prediction_id = p.id
                LEFT JOIN odds ob ON ob.id = (
                    SELECT COALESCE(
                        (SELECT id FROM odds
                         WHERE prediction_id = p.id AND is_bet_snapshot = 1
                           AND DATE(fetched_at) < p.match_date
                         ORDER BY fetched_at DESC LIMIT 1),
                        (SELECT id FROM odds
                         WHERE prediction_id = p.id
                           AND DATE(fetched_at) < p.match_date
                         ORDER BY fetched_at DESC LIMIT 1)
                    )
                )
                WHERE p.id NOT IN (SELECT prediction_id FROM results)
                  AND (
                    l.prediction_id IS NOT NULL
                    OR (p.match_date || ' ' || COALESCE(p.match_time_utc,'23:59'))
                       BETWEEN datetime('now', '-5 hours') AND datetime('now', '+5 minutes')
                  )
                ORDER BY p.match_date, p.match_time_utc
            """).fetchall()
        except Exception:
            live_matches = []

    return (
        [dict(r) for r in value_bets],
        [dict(r) for r in results],
        [dict(r) for r in results_8am],
        [dict(r) for r in results_kickoff],
        dict(stats),
        [dict(r) for r in live_matches],
    )


def ev_color(ev):
    if ev is None: return "#888"
    if ev >= 0.15: return "#16a34a"
    if ev >= 0.07: return "#65a30d"
    if ev >= 0.02: return "#ca8a04"
    if ev < 0:     return "#dc2626"
    return "#64748b"

def ev_bg(ev):
    if ev is None: return ""
    if ev >= 0.15: return "background:rgba(22,163,74,0.18)"
    if ev >= 0.07: return "background:rgba(101,163,13,0.12)"
    if ev >= 0.02: return "background:rgba(202,138,4,0.10)"
    return ""

def comp_flag(comp):
    flags = {
        "LL": "🇪🇸", "EPL": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "LI1": "🇫🇷", "BUN": "🇩🇪",
        "MLS": "🇺🇸", "CHA": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "WSL": "⚽", "LEO": "🌍",
        "SPL": "🏴󠁧󠁢󠁳󠁣󠁴󠁿", "SA": "🇮🇹", "LET": "🌍",
    }
    return flags.get(comp, "⚽")


def _ev_cell(opta, odds, first_odds, is_best_pev):
    """Render a single EV cell (Local / Empate / Visitante)."""
    if not opta or not odds or odds < 1.02 or odds > 25:
        return '<td style="color:#64748b">—</td>'
    ev = (opta / 100) * odds - 1
    if ev > 1.0:
        return '<td style="color:#64748b">—</td>'  # corrupted data

    ev_str = f"{ev:+.1%}"

    # Odds movement: arrow + current odds in color, old odds as muted reference
    odds_color = "#cbd5e1"  # neutral default — visible on dark bg
    arrow = ""
    ref = ""
    if first_odds and abs(odds - first_odds) >= 0.03:
        if odds > first_odds:
            odds_color = "#4ade80"  # green: odds went up (better)
            arrow = "↑"
            ref = f'<span style="color:#64748b;font-size:.75em"> {first_odds:.2f}</span>'
        else:
            odds_color = "#f87171"  # red: odds went down (worse)
            arrow = "↓"
            ref = f'<span style="color:#64748b;font-size:.75em"> {first_odds:.2f}</span>'

    if ev > 0:
        # PEV cell — highlight
        star = " ★" if is_best_pev else ""
        bg = "background:rgba(22,163,74,0.22)" if is_best_pev else "background:rgba(22,163,74,0.10)"
        return (
            f'<td style="{bg};font-weight:bold">'
            f'<span style="color:{ev_color(ev)};font-size:1.05em">{ev_str}{star}</span>'
            f'<br><span style="color:{odds_color};font-size:.8em">{arrow}{odds:.2f}{ref}</span>'
            f'<br><span style="color:#94a3b8;font-size:.75em">{opta:.1f}%</span>'
            f'</td>'
        )
    else:
        # Negative EV — dimmed but readable
        neg_odds_color = odds_color if arrow else "#94a3b8"
        return (
            f'<td style="color:#94a3b8">'
            f'<span style="font-size:.9em">{ev_str}</span>'
            f'<br><span style="color:{neg_odds_color};font-size:.8em">{arrow}{odds:.2f}{ref}</span>'
            f'<br><span style="color:#64748b;font-size:.75em">{opta:.1f}%</span>'
            f'</td>'
        )


def build_value_table(bets):
    """Builds table of ALL upcoming matches with EV per outcome. PEV cells highlighted."""
    if not bets:
        return '<p style="color:#64748b;padding:20px">No hay partidos en el ticker. Espera al próximo scrape.</p>'

    # Sort by Chile datetime ascending (soonest first)
    # Usar hora Chile, no UTC, para que MLS nocturna quede en su día correcto
    sorted_bets = sorted(bets, key=lambda b: (
        *utc_to_chile_dt(b.get("match_time_utc"), b.get("match_date")),
        b["home"]
    ))

    rows = ""
    pev_count = 0
    for b in sorted_bets:
        chile_date, hora = utc_to_chile_dt(b.get("match_time_utc"), b.get("match_date"))
        hora_cell = (
            f'{chile_date}<br>'
            f'<span style="color:#64748b;font-size:.82em">{hora} hs CL</span>'
        ) if hora else (chile_date or b["match_date"])

        # Compute EVs to find best PEV side
        ev_vals = {}
        for side, opta, odds in [
            ("L", b["prob_home"], b["odds_home"]),
            ("E", b["prob_draw"], b["odds_draw"]),
            ("V", b["prob_away"], b["odds_away"]),
        ]:
            if opta and odds and 1.02 <= odds <= 25:
                ev = (opta / 100) * odds - 1
                if 0 < ev <= 1.0:
                    ev_vals[side] = ev
        best_side = max(ev_vals, key=ev_vals.get) if ev_vals else None
        if ev_vals:
            pev_count += 1

        no_odds = not b.get("odds_home")
        if no_odds:
            pending_cell = '<td colspan="3" style="color:#64748b;font-style:italic;text-align:center">Cuotas pendientes</td>'
            rows += f"""
        <tr style="opacity:.6" data-pev="0">
          <td>{b['home_display']}<br>{b['away_display']}</td>
          <td>{comp_flag(b['comp'])} {b['league_display']}</td>
          <td>{hora_cell}</td>
          {pending_cell}
        </tr>"""
            continue

        cell_l = _ev_cell(b["prob_home"], b["odds_home"], b.get("first_odds_home"), best_side == "L")
        cell_e = _ev_cell(b["prob_draw"], b["odds_draw"], b.get("first_odds_draw"), best_side == "E")
        cell_v = _ev_cell(b["prob_away"], b["odds_away"], b.get("first_odds_away"), best_side == "V")

        row_bg = ev_bg(ev_vals.get(best_side)) if best_side else ""
        pev_attr = 'data-pev="1"' if ev_vals else 'data-pev="0"'
        rows += f"""
        <tr style="{row_bg}" {pev_attr}>
          <td>{b['home_display']}<br>{b['away_display']}</td>
          <td>{comp_flag(b['comp'])} {b['league_display']}</td>
          <td>{hora_cell}</td>
          {cell_l}
          {cell_e}
          {cell_v}
        </tr>"""

    total = len(sorted_bets)
    return f"""
    <table id="analysis-table">
      <thead><tr>
        <th>Partido</th><th>Torneo</th><th>Fecha</th>
        <th>Local</th><th>Empate</th><th>Visitante</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_results_table(results):
    """Builds results table with P&L for PEV bets locked at 8pm Chile odds."""
    if not results:
        return '<p style="color:#64748b;padding:20px">Aún no hay resultados registrados.</p>'

    # Ordenar por hora Chile (más reciente primero) — evita que MLS nocturna
    # (UTC mayo 24, Chile mayo 23) quede mezclada con partidos del día siguiente
    results = sorted(results, key=lambda r: (
        *utc_to_chile_dt(r.get("match_time_utc"), r.get("match_date")),
    ), reverse=True)

    total_pl = 0.0
    total_bets = 0
    rows = ""

    for r in results:
        pev_bets = []
        for side, opta, odds in [
            ("L", r["prob_home"], r["odds_home"]),
            ("E", r["prob_draw"], r["odds_draw"]),
            ("V", r["prob_away"], r["odds_away"]),
        ]:
            if not opta or not odds:
                continue
            if odds < 1.02 or odds > 25:
                continue  # sanity check — bad market data
            ev = (opta / 100) * odds - 1
            if ev <= 0 or ev > 1.0:
                continue  # EV > 100% = corrupted data
            pev_bets.append({"side": side, "odds": odds, "ev": ev})

        # Skip matches with no PEV bets
        if not pev_bets:
            continue

        # Keep only the single highest-EV bet
        best_bet = max(pev_bets, key=lambda x: x["ev"])
        won = (r["outcome"] == {"L": "H", "E": "D", "V": "A"}[best_bet["side"]])
        best_bet["won"] = won
        best_bet["pl"] = round(best_bet["odds"] - 1, 3) if won else -1.0
        pev_bets = [best_bet]
        total_pl += best_bet["pl"]
        total_bets += 1

        # Build bet display
        bet_cells = ""
        for b in pev_bets:
            side_name = {"L": "L", "E": "E", "V": "V"}[b["side"]]
            icon = "✅" if b["won"] else "❌"
            pl_color = "#16a34a" if b["pl"] > 0 else "#dc2626"
            pl_str = f"+{b['pl']:.2f}u" if b["pl"] > 0 else f"{b['pl']:.2f}u"
            bet_cells += (
                f'<span style="color:{pl_color};margin-right:10px">'
                f'{icon} {side_name}@{b["odds"]:.2f} <strong>{pl_str}</strong></span>'
            )

        score = f"{r['home_score']}-{r['away_score']}"
        chile_date, hora = utc_to_chile_dt(r.get("match_time_utc"), r.get("match_date"))
        hora_cell = f'{chile_date}<br><span style="color:#64748b;font-size:.82em">{hora} hs CL</span>' if hora else (chile_date or r["match_date"])
        rows += f"""
        <tr>
          <td>{r['home_display']}<br>{r['away_display']}</td>
          <td>{comp_flag(r['comp'])} {r['league_display']}</td>
          <td>{hora_cell}</td>
          <td style="font-weight:bold;font-size:1.1em">{score}</td>
          <td>{bet_cells}</td>
        </tr>"""

    # Summary bar
    roi = (total_pl / total_bets) if total_bets else 0
    roi_color = "var(--green)" if roi >= 0 else "var(--red)"
    roi_str = f"{roi:+.1%}"
    summary = f"""
    <div class="hist-summary">
      <span style="color:var(--muted);font-size:.88em">{total_bets} apuestas registradas</span>
      <span style="font-size:.88em">Rendimiento: <strong style="color:{roi_color};font-size:1.3em">{roi_str}</strong></span>
      <span style="color:var(--dim);font-size:.75em">ROI por apuesta · cuota del día anterior a las 8pm Chile</span>
    </div>""" if total_bets else ""

    return summary + f"""
    <table>
      <thead><tr>
        <th>Partido</th><th>Torneo</th><th>Fecha</th>
        <th>Resultado</th><th>Apuestas PEV → P&L</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_picks_cards(bets):
    """Cards de apuestas recomendadas: una por partido, solo las con PEV > 0."""
    picks = []
    for b in bets:
        best_side, best_ev, best_odds, best_opta = None, 0, 0, 0
        for side, opta, odds in [
            ("L", b["prob_home"], b["odds_home"]),
            ("E", b["prob_draw"], b["odds_draw"]),
            ("V", b["prob_away"], b["odds_away"]),
        ]:
            if not opta or not odds or odds < 1.02 or odds > 25:
                continue
            ev = (opta / 100) * odds - 1
            if 0 < ev <= 1.0 and ev > best_ev:
                best_side, best_ev, best_odds, best_opta = side, ev, odds, opta
        if best_side:
            picks.append({**b, "side": best_side, "ev": best_ev,
                          "odds": best_odds, "opta": best_opta})

    # Sort por fecha UTC (match_date + match_time_utc) — orden cronológico correcto
    picks.sort(key=lambda x: (x["match_date"], x.get("match_time_utc") or "", -x["ev"]))
    if not picks:
        return '<p class="empty-state">No hay apuestas con ventaja en este momento.<br>Vuelve más tarde.</p>'

    side_label = {"L": "Local", "E": "Empate", "V": "Visitante"}
    cards = ""
    # "Hoy" y "Mañana" en fecha Chile (UTC-4 en abr-sep, UTC-3 en oct-mar)
    now_utc = datetime.utcnow()
    chile_today, _ = utc_to_chile_dt(now_utc.strftime("%H:%M"), now_utc.strftime("%Y-%m-%d"))
    chile_tomorrow = (datetime.strptime(chile_today, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    for p in picks:
        chile_date, hora = utc_to_chile_dt(p.get("match_time_utc"), p.get("match_date"))

        # Date label usando fecha Chile real
        if chile_date == chile_today:
            date_label = "Hoy"
        elif chile_date == chile_tomorrow:
            date_label = "Mañana"
        else:
            try:
                dt = datetime.strptime(chile_date, "%Y-%m-%d")
                date_label = f"{dt.day} {dt.strftime('%b')}"
            except Exception:
                date_label = chile_date
        hora_str = f"{date_label} · {hora} hs" if hora else date_label

        # Movement arrow (direction only, sin color)
        side_key = {"L": "home", "E": "draw", "V": "away"}[p["side"]]
        first_odds = p.get(f"first_odds_{side_key}")
        arrow = ""
        if first_odds and abs(p["odds"] - first_odds) >= 0.03:
            arrow = "↑" if p["odds"] > first_odds else "↓"

        ev_pct = f"+{p['ev']:.1%}"
        cards += f"""
        <div class="pick-card">
          <div class="pick-top">
            <span>{comp_flag(p['comp'])} {p['league_display']}</span>
            <span>{hora_str}</span>
          </div>
          <div class="pick-teams">
            <span class="pick-home">{p['home_display']}</span>
            <span class="pick-away">{p['away_display']}</span>
          </div>
          <div class="pick-footer">
            <div class="pick-bet">
              <span class="pick-bet-label">Apostar a</span>
              <span class="pick-bet-side">{side_label[p['side']]}</span>
              <span class="pick-opta">Opta {p['opta']:.1f}%</span>
            </div>
            <div class="pick-nums">
              <span class="pick-ev">{ev_pct}</span>
              <span class="pick-odds">{arrow}{p['odds']:.2f}</span>
            </div>
          </div>
        </div>"""
    return f'<div class="picks-grid">{cards}</div>'


def _roi_stats(results, strategy="best"):
    """
    Calcula (roi, n_bets, first_date) desde una lista de resultados.
    strategy="best"  → apuesta al lado con mayor EV positivo (una por partido)
    strategy="worst" → apuesta al lado con menor EV (mismo set de partidos que "best")
    """
    total_pl, total_bets, first_date = 0.0, 0, None
    for r in results:
        all_valid = []
        for side, opta, odds in [
            ("L", r["prob_home"], r["odds_home"]),
            ("E", r["prob_draw"], r["odds_draw"]),
            ("V", r["prob_away"], r["odds_away"]),
        ]:
            if not opta or not odds or odds < 1.02 or odds > 25:
                continue
            ev = (opta / 100) * odds - 1
            if abs(ev) <= 1.0:
                all_valid.append({"side": side, "odds": odds, "ev": ev})

        pev = [b for b in all_valid if b["ev"] > 0]
        if not pev:
            continue  # solo apostar en partidos donde haya al menos un PEV

        bet = max(pev, key=lambda x: x["ev"]) if strategy == "best" else min(all_valid, key=lambda x: x["ev"])

        won = (r["outcome"] == {"L": "H", "E": "D", "V": "A"}[bet["side"]])
        total_pl += round(bet["odds"] - 1, 3) if won else -1.0
        total_bets += 1
        if not first_date or r["match_date"] < first_date:
            first_date = r["match_date"]

    if not total_bets:
        return None, 0, None
    return total_pl / total_bets, total_bets, first_date


def build_live_section(live_matches):
    """Sección de partidos en juego o pendientes de confirmar resultado."""
    if not live_matches:
        return ""

    STATUS_MAP = {
        "1H":   ("live",    "1ª parte"),
        "2H":   ("live",    "2ª parte"),
        "ET":   ("live",    "Prórroga"),
        "P":    ("live",    "Penales"),
        "LIVE": ("live",    "En juego"),
        "HT":   ("paused",  "Descanso"),
        "BT":   ("paused",  "Pausa"),
        "SUSP": ("paused",  "Suspendido"),
        "INT":  ("paused",  "Interrumpido"),
    }

    has_live = any(m.get("status") in STATUS_MAP and STATUS_MAP[m["status"]][0] == "live"
                   for m in live_matches)

    cards = ""
    for m in live_matches:
        status  = m.get("status")
        hs      = m.get("home_score")
        as_     = m.get("away_score")
        elapsed = m.get("elapsed")

        if status in STATUS_MAP:
            kind, label = STATUS_MAP[status]
            elapsed_str = f" · {elapsed}'" if elapsed else ""
            badge_class = f"live-badge-{kind}"
            badge_html  = f'<span class="live-badge {badge_class}">{label}{elapsed_str}</span>'
            score_html  = f'{hs} – {as_}' if hs is not None else '— – —'
        else:
            # Sin datos de live_scores todavía: partido comenzado pero sin info
            chile_date, hora = utc_to_chile_dt(m.get("match_time_utc"), m.get("match_date"))
            hora_str = f"{hora} hs CL" if hora else chile_date
            badge_html  = f'<span class="live-badge live-badge-pending">⏳ Desde {hora_str}</span>'
            score_html  = '— – —'

        # Calcular apuesta con PEV a las 8pm Chile del día anterior
        pev_html = ""
        side_labels = {"L": "Local", "E": "Empate", "V": "Visit."}
        best_ev, best_side, best_odds = -999, None, None
        for side, opta, odds in [
            ("L", m.get("prob_home"), m.get("bet_odds_home")),
            ("E", m.get("prob_draw"),  m.get("bet_odds_draw")),
            ("V", m.get("prob_away"),  m.get("bet_odds_away")),
        ]:
            if not opta or not odds or odds < 1.02 or odds > 25:
                continue
            ev = (opta / 100) * odds - 1
            if ev > best_ev:
                best_ev, best_side, best_odds = ev, side, odds
        if best_side and best_ev > 0:
            pev_html = (
                f'<span class="live-pev">'
                f'{side_labels[best_side]} ({best_odds:.2f})'
                f'</span>'
            )

        cards += f"""
        <div class="live-card">
          <div class="live-teams">
            <span class="live-team">{m['home_display']}</span>
            <span class="live-score-num">{score_html}</span>
            <span class="live-team away">{m['away_display']}</span>
          </div>
          <div class="live-footer">
            {badge_html}
            <span class="live-league">{m['league_display']}</span>
          </div>
          {('<div class="live-bet">' + pev_html + '</div>') if pev_html else ''}
        </div>"""

    n = len(live_matches)
    label_txt = f"{n} partido{'s' if n != 1 else ''} en curso"
    return f"""
<div class="live-section">
  <div class="container">
    <div class="live-header">
      <span class="section-label">{label_txt}</span>
    </div>
    <div class="live-cards">{cards}
    </div>
  </div>
</div>
<script>
// Auto-recarga cada 2 minutos si hay algún partido en juego
{'(function(){ setTimeout(function(){ location.reload(); }, 120000); })();' if has_live else ''}
</script>"""


def build_stat_bar(results):
    """Banner con ROI acumulado. Retorna '' si no hay datos suficientes."""
    roi, total_bets, first_date = _roi_stats(results, strategy="best")
    if not total_bets:
        return ""
    roi_color = "var(--green)" if roi >= 0 else "var(--red)"
    roi_str = f"{roi:+.1%}"
    try:
        fd = datetime.strptime(first_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        fd = first_date or "—"

    return f"""
<div class="container stat-inner">
  <div class="stat-main">
    <div class="stat-label-small">Rendimiento del modelo</div>
    <div class="stat-roi" style="color:{roi_color}">{roi_str}</div>
  </div>
  <div class="stat-meta">
    {total_bets} apuestas registradas<br>
    <span style="color:var(--dim)">desde {fd}</span>
  </div>
  <a href="history.html" class="stat-expand-hint">Ver historial →</a>
</div>"""


def build_strategy_comparison(r_8pm, r_8am, r_kickoff):
    """Tabla comparando ROI en 3 ventanas de tiempo + anti-modelo."""
    strategies = [
        ("8pm Chile (día anterior)",  r_8pm,      "best"),
        ("8am Chile (día anterior)",  r_8am,      "best"),
        ("Cierre de mercado",         r_kickoff,  "best"),
        ("Anti-modelo (peor EV)",     r_8pm,      "worst"),
    ]
    rows = ""
    for label, results, strategy in strategies:
        roi, n, _ = _roi_stats(results, strategy=strategy)
        if roi is None:
            roi_str = "—"
            roi_color = "var(--dim)"
        else:
            roi_str = f"{roi:+.1%}"
            roi_color = "var(--green)" if roi >= 0 else "var(--red)"

        is_anti = strategy == "worst"
        row_style = "opacity:.65" if is_anti else ""
        border_top = "border-top:2px solid var(--border2)" if is_anti else ""
        rows += f"""
        <tr style="{row_style}">
          <td style="{border_top}">{label}</td>
          <td style="{border_top};text-align:center;color:var(--muted)">{n if n else "—"}</td>
          <td style="{border_top};text-align:right;font-weight:700;color:{roi_color}">{roi_str}</td>
        </tr>"""

    return f"""
    <div style="padding:16px 20px;border-bottom:1px solid var(--border)">
      <div style="font-size:.65em;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
                  color:var(--dim);margin-bottom:12px">Comparativa de estrategias</div>
      <table style="width:auto;font-size:.85em">
        <thead>
          <tr>
            <th style="text-align:left;padding:4px 16px 4px 0;color:var(--dim);font-weight:600;
                       font-size:.75em;letter-spacing:.05em;text-transform:uppercase">Estrategia</th>
            <th style="text-align:center;padding:4px 16px;color:var(--dim);font-weight:600;
                       font-size:.75em;letter-spacing:.05em;text-transform:uppercase">Apuestas</th>
            <th style="text-align:right;padding:4px 0 4px 16px;color:var(--dim);font-weight:600;
                       font-size:.75em;letter-spacing:.05em;text-transform:uppercase">ROI</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>"""


def generate():
    bets, results, results_8am, results_kickoff, stats, live_matches = load_data()
    now = datetime.now().strftime("%d/%m/%Y %H:%M")

    stat_bar      = build_stat_bar(results)
    live_html     = build_live_section(live_matches)
    analysis_html = build_value_table(bets)

    n_pev = sum(
        1 for b in bets
        if any(
            opta and odds and 0 < (opta / 100) * odds - 1 <= 1.0
            for _, opta, odds in [
                ("L", b["prob_home"], b["odds_home"]),
                ("E", b["prob_draw"], b["odds_draw"]),
                ("V", b["prob_away"], b["odds_away"]),
            ]
        )
    )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="300">
<title>Value Bets · Opta</title>
<style>
  :root {{
    --bg:#0d1117; --surface:#161b22; --surface2:#1c2128;
    --border:#30363d; --border2:#21262d;
    --green:#3fb950; --green-dim:rgba(63,185,80,0.12);
    --red:#f85149; --yellow:#d29922;
    --text:#e6edf3; --muted:#8b949e; --dim:#484f58;
  }}
  *, *::before, *::after {{ box-sizing:border-box; margin:0; padding:0 }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,sans-serif;
         background:var(--bg); color:var(--text); min-height:100vh; line-height:1.5 }}

  /* ── LAYOUT ── */
  .container {{ max-width:1100px; margin:0 auto; padding:0 24px }}

  /* ── HEADER ── */
  .site-header {{ padding:28px 0 0 }}
  .header-inner {{ display:flex; justify-content:space-between; align-items:baseline;
                  flex-wrap:wrap; gap:6px }}
  .header-title {{ font-size:1.1em; font-weight:800; letter-spacing:-.01em }}
  .header-title .accent {{ color:var(--green) }}
  .header-meta {{ font-size:.75em; color:var(--dim) }}
  .dot-live {{ display:inline-block; width:7px; height:7px; border-radius:50%;
              background:var(--green); box-shadow:0 0 6px var(--green);
              animation:blink 2s infinite; margin-right:4px; vertical-align:middle }}
  @keyframes blink {{ 0%,100% {{ opacity:1 }} 50% {{ opacity:.3 }} }}

  /* ── STAT BAR ── */
  .stat-bar {{ background:var(--surface); border-top:1px solid var(--border2);
              border-bottom:1px solid var(--border2); margin-top:20px }}
  .stat-inner {{ display:flex; align-items:center; gap:32px; flex-wrap:wrap;
                padding:20px 0 }}
  .stat-label-small {{ font-size:.65em; font-weight:700; letter-spacing:.12em;
                      text-transform:uppercase; color:var(--dim); margin-bottom:4px }}
  .stat-roi {{ font-size:2.2em; font-weight:800; line-height:1 }}
  .stat-meta {{ font-size:.82em; color:var(--muted); line-height:1.7 }}
  .stat-expand-hint {{ margin-left:auto; display:flex; align-items:center; gap:6px;
                       color:var(--muted); font-size:.78em; font-weight:600;
                       border:1px solid var(--border); border-radius:6px;
                       padding:5px 10px; transition:all .15s; white-space:nowrap;
                       text-decoration:none }}
  .stat-expand-hint:hover {{ color:var(--text); border-color:var(--muted) }}

  /* ── SECTION HEADER ── */
  .section-header {{ display:flex; justify-content:space-between; align-items:center;
                    margin:32px 0 14px; padding-bottom:8px;
                    border-bottom:1px solid var(--border2) }}
  .section-label {{ font-size:.68em; font-weight:700; letter-spacing:.13em;
                   text-transform:uppercase; color:var(--muted) }}
  .filter-btn {{ font-size:.75em; font-weight:600; padding:5px 12px; cursor:pointer;
                background:transparent; color:var(--muted); border:1px solid var(--border);
                border-radius:6px; transition:all .15s }}
  .filter-btn:hover {{ color:var(--text); border-color:var(--muted) }}
  .filter-btn.active {{ color:var(--green); border-color:var(--green) }}

  /* ── ANALYSIS TABLE FILTER ── */
  #analysis-table tbody tr[data-pev="0"] {{ display:none }}
  #analysis-table.show-all tbody tr[data-pev="0"] {{ display:table-row }}

  /* ── DETAILS / COLLAPSIBLE ── */
  details {{
    background:var(--surface); border:1px solid var(--border);
    border-radius:8px; overflow:hidden;
  }}
  details + details {{ margin-top:10px }}
  details summary {{
    cursor:pointer; padding:16px 20px;
    font-size:.88em; font-weight:600; color:var(--muted);
    list-style:none; user-select:none;
    display:flex; align-items:center; gap:8px;
  }}
  details summary::-webkit-details-marker {{ display:none }}
  details summary::before {{
    content:"▶"; font-size:.65em; color:var(--dim);
    transition:transform .2s; flex-shrink:0;
  }}
  details[open] summary::before {{ transform:rotate(90deg) }}
  details summary:hover {{ background:var(--surface2); color:var(--text) }}
  details[open] summary {{ border-bottom:1px solid var(--border); color:var(--text) }}
  .details-inner {{ overflow-x:auto }}

  /* ── TABLE ── */
  table {{ width:100%; border-collapse:collapse; font-size:.85em }}
  th {{ background:var(--surface); color:var(--muted); font-weight:600;
       text-transform:uppercase; font-size:.68em; letter-spacing:.08em;
       padding:10px 16px; text-align:left; border-bottom:1px solid var(--border) }}
  td {{ padding:11px 16px; border-bottom:1px solid var(--border2);
       color:var(--text); vertical-align:middle }}
  tr:last-child td {{ border-bottom:none }}
  tr:hover td {{ background:rgba(255,255,255,.025) }}

  /* ── LEGEND ── */
  .legend {{ display:flex; gap:16px; padding:12px 16px; flex-wrap:wrap;
            font-size:.75em; color:var(--muted); background:var(--surface);
            border-top:1px solid var(--border2) }}

  /* ── LIVE SECTION ── */
  .live-section {{ background:var(--surface); border-top:1px solid var(--border2);
                  border-bottom:1px solid var(--border2); padding:16px 0 20px }}
  .live-header  {{ margin-bottom:14px }}
  .live-cards   {{ display:flex; gap:12px; flex-wrap:wrap }}
  .live-card    {{ background:var(--bg); border:1px solid var(--border); border-radius:10px;
                  padding:14px 18px; min-width:210px; flex:1; max-width:320px }}
  .live-teams   {{ display:flex; align-items:center; justify-content:space-between;
                  gap:8px; margin-bottom:10px }}
  .live-team    {{ font-size:.88em; font-weight:600; color:var(--text); flex:1 }}
  .live-team.away {{ text-align:right }}
  .live-score-num {{ font-size:1.35em; font-weight:800; color:var(--text);
                     white-space:nowrap; text-align:center; flex-shrink:0 }}
  .live-footer  {{ display:flex; align-items:center; justify-content:space-between; gap:8px }}
  .live-badge   {{ font-size:.70em; font-weight:700; border-radius:4px; padding:2px 8px }}
  .live-badge-live    {{ color:#ef4444; background:rgba(239,68,68,.14) }}
  .live-badge-paused  {{ color:#f59e0b; background:rgba(245,158,11,.12) }}
  .live-badge-pending {{ color:#64748b; background:rgba(100,116,139,.10) }}
  .live-league  {{ font-size:.70em; color:var(--dim); text-align:right }}
  .live-bet     {{ margin-top:8px; padding-top:8px; border-top:1px solid var(--border2) }}
  .live-pev     {{ font-size:.75em; font-weight:700; color:var(--green) }}

  /* ── HISTORY SUMMARY ── */
  .hist-summary {{ padding:16px 20px; background:var(--surface);
                  display:flex; gap:28px; flex-wrap:wrap; align-items:center;
                  border-bottom:1px solid var(--border) }}
</style>
</head>
<body>

<div class="site-header">
  <div class="container">
    <div class="header-inner">
      <div class="header-title">Value Bets <span class="accent">·</span> Opta</div>
      <div class="header-meta"><span class="dot-live"></span>{now} · {stats['total']} partidos analizados</div>
    </div>
  </div>
</div>

{"<div class='stat-bar'>" + stat_bar + "</div>" if stat_bar else ""}

{live_html}

<div class="container" style="padding-bottom:60px">

  <div class="section-header">
    <div class="section-label">{n_pev} con ventaja · {len(bets)} analizados</div>
    <button id="filter-btn" class="filter-btn" onclick="toggleFilter()">Ver todos</button>
  </div>

  <div class="table-wrap">
    {analysis_html}
    <div class="legend">
      <span>★ = mejor apuesta del partido</span>
      <span>·</span>
      <span><span style="color:var(--green)">↑</span> cuota subió (mejor)</span>
      <span>·</span>
      <span><span style="color:var(--red)">↓</span> cuota bajó (mercado lo corrigió)</span>
    </div>
  </div>

</div>

<script>
function toggleFilter() {{
  const tbl = document.getElementById('analysis-table');
  const btn = document.getElementById('filter-btn');
  const showAll = tbl.classList.toggle('show-all');
  btn.textContent = showAll ? 'Solo con ventaja' : 'Ver todos';
  btn.classList.toggle('active', showAll);
}}
</script>
</body>
</html>"""

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"  Dashboard generado → {OUTPUT}")

    generate_history(results, results_8am, results_kickoff)


def generate_history(results, results_8am, results_kickoff):
    """Genera docs/history.html con comparativa de estrategias + historial partido a partido."""
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    comparison_html = build_strategy_comparison(results, results_8am, results_kickoff)
    history_html    = build_results_table(results)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Historial · Opta</title>
<style>
  :root {{
    --bg:#0d1117; --surface:#161b22; --surface2:#1c2128;
    --border:#30363d; --border2:#21262d;
    --green:#3fb950; --red:#f85149; --yellow:#d29922;
    --text:#e6edf3; --muted:#8b949e; --dim:#484f58;
  }}
  *, *::before, *::after {{ box-sizing:border-box; margin:0; padding:0 }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,sans-serif;
         background:var(--bg); color:var(--text); min-height:100vh; line-height:1.5 }}
  .container {{ max-width:1100px; margin:0 auto; padding:0 24px }}
  .site-header {{ padding:28px 0 20px; border-bottom:1px solid var(--border2) }}
  .header-inner {{ display:flex; justify-content:space-between; align-items:baseline;
                  flex-wrap:wrap; gap:12px }}
  .back-link {{ font-size:.82em; color:var(--muted); text-decoration:none;
               display:flex; align-items:center; gap:5px; transition:color .15s }}
  .back-link:hover {{ color:var(--text) }}
  .header-title {{ font-size:1.1em; font-weight:800 }}
  .header-meta {{ font-size:.75em; color:var(--dim) }}
  table {{ width:100%; border-collapse:collapse; font-size:.85em }}
  th {{ background:var(--surface); color:var(--muted); font-weight:600;
       text-transform:uppercase; font-size:.68em; letter-spacing:.08em;
       padding:10px 16px; text-align:left; border-bottom:1px solid var(--border) }}
  td {{ padding:11px 16px; border-bottom:1px solid var(--border2);
       color:var(--text); vertical-align:middle }}
  tr:last-child td {{ border-bottom:none }}
  tr:hover td {{ background:rgba(255,255,255,.025) }}
  .hist-summary {{ padding:16px 20px; background:var(--surface);
                  display:flex; gap:28px; flex-wrap:wrap; align-items:center;
                  border-bottom:1px solid var(--border) }}
  .section-label {{ font-size:.68em; font-weight:700; letter-spacing:.13em;
                   text-transform:uppercase; color:var(--muted);
                   margin:32px 0 14px; padding-bottom:8px;
                   border-bottom:1px solid var(--border2) }}
  .table-wrap {{ border:1px solid var(--border); border-radius:8px; overflow:hidden; overflow-x:auto }}
</style>
</head>
<body>

<div class="site-header">
  <div class="container">
    <div class="header-inner">
      <a href="index.html" class="back-link">← Volver al dashboard</a>
      <div class="header-title">Historial de apuestas</div>
      <div class="header-meta">{now}</div>
    </div>
  </div>
</div>

<div class="container" style="padding-bottom:60px">

  <div class="section-label">Comparativa de estrategias</div>
  <div class="table-wrap">
    {comparison_html}
  </div>

  <div class="section-label">Apuesta a apuesta · cuota 8pm Chile día anterior</div>
  <div class="table-wrap">
    {history_html}
  </div>

</div>
</body>
</html>"""

    HISTORY_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_OUTPUT.write_text(html, encoding="utf-8")
    print(f"  Historial generado  → {HISTORY_OUTPUT}")


if __name__ == "__main__":
    generate()
