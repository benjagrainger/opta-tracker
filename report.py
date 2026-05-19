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

        stats = conn.execute("""
            SELECT COUNT(*) as total FROM predictions
        """).fetchone()

    return (
        [dict(r) for r in value_bets],
        [dict(r) for r in results],
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

    # Odds movement: color the current odds, show old odds as reference
    odds_color = "#cbd5e1"  # neutral default — visible on dark bg
    move = ""
    if first_odds and abs(odds - first_odds) >= 0.03:
        if odds > first_odds:
            odds_color = "#4ade80"  # green: odds went up (better)
            move = f'<span style="color:#94a3b8;font-size:.75em"> ↑{first_odds:.2f}</span>'
        else:
            odds_color = "#f87171"  # red: odds went down (worse)
            move = f'<span style="color:#94a3b8;font-size:.75em"> ↓{first_odds:.2f}</span>'

    if ev > 0:
        # PEV cell — highlight
        star = " ★" if is_best_pev else ""
        bg = "background:rgba(22,163,74,0.22)" if is_best_pev else "background:rgba(22,163,74,0.10)"
        return (
            f'<td style="{bg};font-weight:bold">'
            f'<span style="color:{ev_color(ev)};font-size:1.05em">{ev_str}{star}</span>'
            f'<br><span style="color:{odds_color};font-size:.8em">{odds:.2f}{move}</span>'
            f'<br><span style="color:#94a3b8;font-size:.75em">{opta:.1f}%</span>'
            f'</td>'
        )
    else:
        # Negative EV — dimmed but readable
        neg_odds_color = odds_color if move else "#94a3b8"
        return (
            f'<td style="color:#94a3b8">'
            f'<span style="font-size:.9em">{ev_str}</span>'
            f'<br><span style="color:{neg_odds_color};font-size:.8em">{odds:.2f}{move}</span>'
            f'<br><span style="color:#64748b;font-size:.75em">{opta:.1f}%</span>'
            f'</td>'
        )


def build_value_table(bets):
    """Builds table of ALL upcoming matches with EV per outcome. PEV cells highlighted."""
    if not bets:
        return '<p style="color:#64748b;padding:20px">No hay partidos en el ticker. Esperá al próximo scrape.</p>'

    # Pre-sort: matches with PEV first, then by date/time
    def match_sort_key(b):
        evs = []
        for opta, odds in [(b["prob_home"], b["odds_home"]),
                           (b["prob_draw"], b["odds_draw"]),
                           (b["prob_away"], b["odds_away"])]:
            if opta and odds and 1.02 <= odds <= 25:
                ev = (opta / 100) * odds - 1
                if 0 < ev <= 1.0:
                    evs.append(ev)
        has_pev = 1 if evs else 0
        best_ev = max(evs) if evs else 0
        return (-has_pev, -best_ev, b["match_date"], b.get("match_time_utc") or "")

    sorted_bets = sorted(bets, key=match_sort_key)

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
    roi_color = "#16a34a" if roi >= 0 else "#dc2626"
    roi_str = f"{roi:+.1%}"
    summary = f"""
    <div style="padding:14px 20px;background:#0f172a;border-bottom:1px solid #334155;
                display:flex;align-items:center;gap:24px;flex-wrap:wrap">
      <span style="color:#94a3b8">{total_bets} apuestas PEV</span>
      <span>Rendimiento: <strong style="color:{roi_color};font-size:1.2em">{roi_str}</strong></span>
      <span style="color:#64748b;font-size:.8em">ROI por apuesta · 📸 = cuota 8pm Chile día anterior</span>
    </div>""" if total_bets else ""

    return summary + f"""
    <table>
      <thead><tr>
        <th>Partido</th><th>Torneo</th><th>Fecha</th>
        <th>Resultado</th><th>Apuestas PEV → P&L</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def generate():
    bets, results, stats = load_data()
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    next_scrape = "8:00 / 14:00 / 20:00 hs Chile (automático)"

    # Count PEV bets in current ticker
    pev_count = sum(
        1 for b in bets
        for side, opta, odds in [
            ("L", b["prob_home"], b["odds_home"]),
            ("E", b["prob_draw"], b["odds_draw"]),
            ("V", b["prob_away"], b["odds_away"]),
        ]
        if opta and odds and (opta / 100) * odds - 1 > 0
    )

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="600">
<title>Opta Tracker</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0 }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         background:#0f172a; color:#e2e8f0; min-height:100vh }}
  .header {{ background:linear-gradient(135deg,#1e3a5f,#2d1b69);
             padding:24px 32px; border-bottom:1px solid #1e293b }}
  .header h1 {{ font-size:1.8em; font-weight:700; color:#fff }}
  .header p  {{ color:#94a3b8; font-size:.9em; margin-top:4px }}
  .badges {{ display:flex; gap:10px; margin-top:12px; flex-wrap:wrap }}
  .badge {{ background:#1e293b; border:1px solid #334155; border-radius:99px;
            padding:4px 12px; font-size:.8em; color:#94a3b8 }}
  .badge strong {{ color:#e2e8f0 }}
  .badge.green {{ border-color:#16a34a; background:rgba(22,163,74,0.12) }}
  .badge.green strong {{ color:#4ade80 }}
  .container {{ max-width:1200px; margin:0 auto; padding:24px 16px }}
  .section {{ background:#1e293b; border-radius:12px; margin-bottom:24px;
              border:1px solid #334155; overflow:hidden }}
  .section-header {{ padding:16px 20px; border-bottom:1px solid #334155;
                     display:flex; align-items:center; gap:10px }}
  .section-header h2 {{ font-size:1.1em; font-weight:600; color:#f1f5f9 }}
  .section-header .dot {{ width:8px;height:8px;border-radius:50%;background:#22c55e;
                          box-shadow:0 0 6px #22c55e; animation:pulse 2s infinite }}
  @keyframes pulse {{ 0%,100%{{opacity:1}}50%{{opacity:.4}} }}
  .section-body {{ padding:0; overflow-x:auto }}
  table {{ width:100%; border-collapse:collapse; font-size:.88em }}
  th {{ background:#0f172a; color:#94a3b8; font-weight:600; text-transform:uppercase;
        font-size:.75em; letter-spacing:.05em; padding:10px 14px; text-align:left;
        border-bottom:1px solid #334155 }}
  td {{ padding:10px 14px; border-bottom:1px solid #1e3a5f; vertical-align:middle;
       color:#e2e8f0 }}
  tr:last-child td {{ border-bottom:none }}
  tr:hover td {{ background:rgba(255,255,255,.04) }}
  .legend {{ display:flex; gap:16px; padding:14px 20px; flex-wrap:wrap;
             background:#0f172a; border-top:1px solid #1e293b; font-size:.8em;
             color:#94a3b8 }}
</style>
</head>
<body>
<div class="header">
  <h1>⚽ Opta Tracker</h1>
  <p>Probabilidades Opta vs mercado — solo apuestas con valor esperado positivo</p>
  <div class="badges">
    <span class="badge">🕐 Actualizado: <strong>{now}</strong></span>
    <span class="badge">🔄 Próximo scrape: <strong>{next_scrape}</strong></span>
    <span class="badge">📊 Partidos en DB: <strong>{stats['total']}</strong></span>
    <span class="badge {'green' if pev_count else ''}">
      🎯 PEV activos: <strong>{pev_count}</strong>
    </span>
  </div>
</div>

<div class="container">

  <div class="section">
    <div class="section-header">
      <div class="dot"></div>
      <h2>Partidos analizados — EV por resultado (★ = apuesta recomendada)</h2>
    </div>
    <div class="section-body">
      {build_value_table(bets)}
    </div>
    <div class="legend">
      <span>Cada celda: EV · cuota actual · % Opta</span>
      <span>·</span>
      <span>★ = mejor apuesta del partido (mayor PEV)</span>
      <span>·</span>
      <span><span style="color:#4ade80">↑ cuota</span> = subió desde 1ª detección (mejor) · <span style="color:#f87171">↓ cuota</span> = bajó (mercado corrigió)</span>
    </div>
  </div>

  <div class="section">
    <div class="section-header">
      <h2>📋 Historial — partidos jugados</h2>
    </div>
    <div class="section-body">
      {build_results_table(results)}
    </div>
  </div>

</div>
</body>
</html>"""

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"  Dashboard generado → {OUTPUT}")


if __name__ == "__main__":
    generate()
