"""Genera el dashboard HTML. Se llama automáticamente después de cada scrape."""
from datetime import datetime, timedelta
from pathlib import Path
from db import get_conn


def utc_to_chile(time_str, date_str=None):
    """Convert 'HH:MM' UTC to Chile time.
    Apr–Sep: CLT (UTC-4) · Oct–Mar: CLST (UTC-3)
    date_str='YYYY-MM-DD' lets us use the match's month; falls back to today.
    """
    if not time_str:
        return ""
    try:
        h, m = map(int, time_str.split(":"))
        month = int(date_str[5:7]) if date_str else datetime.now().month
        offset = 4 if 4 <= month <= 9 else 3   # CLT vs CLST
        t = timedelta(hours=h, minutes=m) - timedelta(hours=offset)
        total = int(t.total_seconds())
        if total < 0:
            total += 86400
        return f"{total // 3600:02d}:{(total % 3600) // 60:02d}"
    except Exception:
        return time_str

OUTPUT = Path(__file__).parent / "docs" / "index.html"


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
            JOIN odds o ON o.id = (SELECT MAX(id) FROM odds WHERE prediction_id = p.id)
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
            ORDER BY p.match_date DESC
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

    return (
        [dict(r) for r in value_bets],
        [dict(r) for r in results],
        [dict(r) for r in results_8am],
        [dict(r) for r in results_kickoff],
        dict(stats),
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

    # Sort by date/time ascending (soonest first)
    sorted_bets = sorted(bets, key=lambda b: (b["match_date"], b.get("match_time_utc") or "", b["home"]))

    rows = ""
    pev_count = 0
    for b in sorted_bets:
        hora = utc_to_chile(b.get("match_time_utc"), b.get("match_date"))
        hora_cell = (
            f'{b["match_date"]}<br>'
            f'<span style="color:#64748b;font-size:.82em">{hora} hs CL</span>'
        ) if hora else b["match_date"]

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

        cell_l = _ev_cell(b["prob_home"], b["odds_home"], b.get("first_odds_home"), best_side == "L")
        cell_e = _ev_cell(b["prob_draw"], b["odds_draw"], b.get("first_odds_draw"), best_side == "E")
        cell_v = _ev_cell(b["prob_away"], b["odds_away"], b.get("first_odds_away"), best_side == "V")

        row_bg = ev_bg(ev_vals.get(best_side)) if best_side else ""
        rows += f"""
        <tr style="{row_bg}">
          <td>{b['home_display']}<br>{b['away_display']}</td>
          <td>{comp_flag(b['comp'])} {b['league_display']}</td>
          <td>{hora_cell}</td>
          {cell_l}
          {cell_e}
          {cell_v}
        </tr>"""

    total = len(sorted_bets)
    pev_note = (
        f'<span style="color:#4ade80;font-weight:bold">{pev_count} con PEV ★</span>'
        f' · <span style="color:#475569">{total - pev_count} sin valor</span>'
    )
    return f"""
    <div style="padding:10px 20px;background:#0f172a;border-bottom:1px solid #334155;
                font-size:.82em;color:#64748b">
      {total} partidos analizados · {pev_note}
    </div>
    <table>
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
        hora = utc_to_chile(r.get("match_time_utc"), r.get("match_date"))
        hora_cell = f'{r["match_date"]}<br><span style="color:#64748b;font-size:.82em">{hora} hs CL</span>' if hora else r["match_date"]
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

    picks.sort(key=lambda x: (x["match_date"], x.get("match_time_utc") or "", -x["ev"]))
    if not picks:
        return '<p class="empty-state">No hay apuestas con ventaja en este momento.<br>Vuelve más tarde.</p>'

    side_label = {"L": "Local", "E": "Empate", "V": "Visitante"}
    cards = ""
    today = datetime.now().strftime("%Y-%m-%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    for p in picks:
        hora = utc_to_chile(p.get("match_time_utc"), p.get("match_date"))

        # Date label
        if p["match_date"] == today:
            date_label = "Hoy"
        elif p["match_date"] == tomorrow:
            date_label = "Mañana"
        else:
            try:
                dt = datetime.strptime(p["match_date"], "%Y-%m-%d")
                date_label = f"{dt.day} {dt.strftime('%b')}"
            except Exception:
                date_label = p["match_date"]
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
  <div class="stat-expand-hint">
    <span class="stat-hint-text">Ver historial</span>
    <span class="stat-arrow">▾</span>
  </div>
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
    bets, results, results_8am, results_kickoff, stats = load_data()
    now = datetime.now().strftime("%d/%m/%Y %H:%M")

    stat_bar        = build_stat_bar(results)
    picks_html      = build_picks_cards(bets)
    analysis_html   = build_value_table(bets)
    history_html    = build_results_table(results)
    comparison_html = build_strategy_comparison(results, results_8am, results_kickoff)

    # Count matches (not cells) with at least one PEV side
    n_picks = sum(
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

    # Count results that had at least one PEV bet
    n_results = sum(
        1 for r in results
        if any(
            opta and odds and 0 < (opta / 100) * odds - 1 <= 1.0
            for _, opta, odds in [
                ("L", r["prob_home"], r["odds_home"]),
                ("E", r["prob_draw"], r["odds_draw"]),
                ("V", r["prob_away"], r["odds_away"]),
            ]
        )
    )

    picks_label = (
        f"{n_picks} apuesta{'s' if n_picks != 1 else ''} con ventaja ahora"
        if n_picks else "Sin apuestas con ventaja en este momento"
    )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="600">
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

  /* ── STAT BAR (clickable) ── */
  .stat-details {{ background:var(--surface); border-top:1px solid var(--border2);
                  border-bottom:1px solid var(--border2); margin-top:20px }}
  .stat-details > summary {{ list-style:none; cursor:pointer; display:block }}
  .stat-details > summary::-webkit-details-marker {{ display:none }}
  .stat-details > summary::before {{ display:none }}
  .stat-details > summary:hover .stat-inner {{ background:var(--surface2) }}
  .stat-details[open] > summary .stat-inner {{ border-bottom:1px solid var(--border) }}
  .stat-inner {{ display:flex; align-items:center; gap:32px; flex-wrap:wrap;
                padding:20px 0 }}
  .stat-label-small {{ font-size:.65em; font-weight:700; letter-spacing:.12em;
                      text-transform:uppercase; color:var(--dim); margin-bottom:4px }}
  .stat-roi {{ font-size:2.2em; font-weight:800; line-height:1 }}
  .stat-meta {{ font-size:.82em; color:var(--muted); line-height:1.7 }}
  .stat-expand-hint {{ margin-left:auto; display:flex; align-items:center; gap:6px;
                       color:var(--muted); font-size:.78em; font-weight:600;
                       border:1px solid var(--border); border-radius:6px;
                       padding:5px 10px; transition:all .15s; white-space:nowrap }}
  .stat-details > summary:hover .stat-expand-hint {{ color:var(--text); border-color:var(--muted) }}
  .stat-arrow {{ font-size:1em; transition:transform .25s; display:inline-block }}
  .stat-details[open] .stat-arrow {{ transform:rotate(180deg) }}
  .stat-details[open] .stat-expand-hint {{ color:var(--text); border-color:var(--muted) }}

  /* ── SECTION LABEL ── */
  .section-label {{ font-size:.68em; font-weight:700; letter-spacing:.13em;
                   text-transform:uppercase; color:var(--muted);
                   margin:32px 0 14px; padding-bottom:8px;
                   border-bottom:1px solid var(--border2) }}

  /* ── PICK CARDS ── */
  .picks-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(270px,1fr)); gap:12px }}
  .pick-card {{
    background:var(--surface); border:1px solid var(--border);
    border-radius:10px; padding:16px 18px;
    display:flex; flex-direction:column;
    transition:border-color .15s, transform .1s;
  }}
  .pick-card:hover {{ border-color:var(--green); transform:translateY(-1px) }}
  .pick-top {{ display:flex; justify-content:space-between; align-items:center;
              font-size:.72em; color:var(--muted); margin-bottom:12px }}
  .pick-teams {{ display:flex; flex-direction:column; gap:3px; margin-bottom:14px; flex:1 }}
  .pick-home {{ font-size:.95em; font-weight:600 }}
  .pick-away {{ font-size:.95em; font-weight:600 }}
  .pick-footer {{ display:flex; justify-content:space-between; align-items:flex-end;
                 padding-top:12px; border-top:1px solid var(--border2) }}
  .pick-bet {{ display:flex; flex-direction:column; gap:2px }}
  .pick-bet-label {{ font-size:.62em; text-transform:uppercase; letter-spacing:.1em; color:var(--dim) }}
  .pick-bet-side {{ font-size:.9em; font-weight:700 }}
  .pick-opta {{ font-size:.7em; color:var(--muted) }}
  .pick-nums {{ display:flex; flex-direction:column; align-items:flex-end; gap:3px }}
  .pick-ev {{ font-size:1.45em; font-weight:800; color:var(--green); line-height:1 }}
  .pick-odds {{ font-size:.8em; color:var(--muted) }}
  .empty-state {{ color:var(--muted); padding:28px 0; font-size:.9em; line-height:1.8 }}

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

{"<details class='stat-details'><summary>" + stat_bar + "</summary><div class='details-inner'>" + comparison_html + history_html + "</div></details>" if stat_bar else ""}

<div class="container" style="padding-bottom:60px">

  <div class="section-label">{picks_label}</div>
  {picks_html}

  <div style="margin-top:32px"></div>

  <details id="analisis">
    <summary>Análisis técnico · {len(bets)} partidos próximos</summary>
    <div class="details-inner">
      {analysis_html}
      <div class="legend">
        <span>★ = apuesta recomendada del partido</span>
        <span>·</span>
        <span><span style="color:var(--green)">↑</span> cuota subió desde la primera detección (mejor)</span>
        <span>·</span>
        <span><span style="color:var(--red)">↓</span> cuota bajó (mercado lo corrigió)</span>
      </div>
    </div>
  </details>

</div>
</body>
</html>"""

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"  Dashboard generado → {OUTPUT}")


if __name__ == "__main__":
    generate()
