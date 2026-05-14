"""Genera el dashboard HTML. Se llama automáticamente después de cada scrape."""
from datetime import datetime
from pathlib import Path
from db import get_conn

OUTPUT = Path(__file__).parent / "docs" / "index.html"


def load_data():
    with get_conn() as conn:
        # Current value bets: latest odds per prediction, no result yet
        value_bets = conn.execute("""
            SELECT p.comp, p.home, p.away, p.match_date,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away,
                   o.fetched_at
            FROM predictions p
            JOIN odds o ON o.prediction_id = p.id
            WHERE o.id IN (SELECT MAX(id) FROM odds GROUP BY prediction_id)
              AND p.id NOT IN (SELECT prediction_id FROM results)
            ORDER BY p.match_date, p.home
        """).fetchall()

        # Results: use "bet odds" = latest 22-23h snapshot from day before match,
        # falling back to earliest available odds for that prediction
        results = conn.execute("""
            SELECT p.comp, p.home, p.away, p.match_date,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away,
                   r.home_score, r.away_score, r.outcome
            FROM predictions p
            JOIN results r ON r.prediction_id = p.id
            JOIN odds o ON o.id = (
                SELECT COALESCE(
                    (SELECT id FROM odds
                     WHERE prediction_id = p.id
                       AND strftime('%H', fetched_at) IN ('22','23')
                       AND DATE(fetched_at) = DATE(p.match_date, '-1 day')
                     ORDER BY fetched_at DESC LIMIT 1),
                    (SELECT MIN(id) FROM odds WHERE prediction_id = p.id)
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


def build_value_table(bets):
    """Builds table of matches with at least one side with EV > 0."""
    candidates = []
    for b in bets:
        for side, opta, odds in [
            ("L", b["prob_home"], b["odds_home"]),
            ("E", b["prob_draw"], b["odds_draw"]),
            ("V", b["prob_away"], b["odds_away"]),
        ]:
            if not opta or not odds:
                continue
            if odds < 1.02 or odds > 25:
                continue  # sanity: implausible odds → wrong market data
            ev = (opta / 100) * odds - 1
            if ev <= 0 or ev > 1.0:
                continue  # EV > 100% = corrupted data
            team = b["home"] if side == "L" else (b["away"] if side == "V" else "Empate")
            candidates.append({
                    **b,
                    "side": side,
                    "team": team,
                    "opta": opta,
                    "odds": odds,
                    "ev": ev,
                })

    candidates.sort(key=lambda x: (x["match_date"], -x["ev"]))

    if not candidates:
        return '<p style="color:#64748b;padding:20px">No hay apuestas con PEV. Esperá al próximo scrape.</p>'

    rows = ""
    for c in candidates:
        ev_str = f"{c['ev']:+.1%}"
        side_label = {"L": "Local", "E": "Empate", "V": "Visitante"}[c["side"]]
        rows += f"""
        <tr style="{ev_bg(c['ev'])}">
          <td>{comp_flag(c['comp'])} {c['comp']}</td>
          <td>{c['match_date']}</td>
          <td><strong>{c['home']}</strong> vs <strong>{c['away']}</strong></td>
          <td>{side_label}: <strong>{c['team']}</strong></td>
          <td style="font-size:1.1em;font-weight:bold">{c['odds']:.2f}</td>
          <td>{c['opta']:.1f}%</td>
          <td style="color:{ev_color(c['ev'])};font-weight:bold;font-size:1.1em">{ev_str}</td>
        </tr>"""

    return f"""
    <table>
      <thead><tr>
        <th>Liga</th><th>Fecha</th><th>Partido</th><th>Apuesta</th>
        <th>Cuota</th><th>Opta %</th><th>EV</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_results_table(results):
    """Builds results table with P&L for PEV bets locked at 8pm Chile odds."""
    if not results:
        return '<p style="color:#64748b;padding:20px">Aún no hay resultados registrados.</p>'

    total_pl = 0.0
    total_bets = 0
    wins = 0
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
            won = (r["outcome"] == {"L": "H", "E": "D", "V": "A"}[side])
            pl = round(odds - 1, 3) if won else -1.0
            pev_bets.append({
                "side": side, "odds": odds, "ev": ev, "won": won, "pl": pl
            })
            total_pl += pl
            total_bets += 1
            if won: wins += 1

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
        rows += f"""
        <tr>
          <td>{comp_flag(r['comp'])} {r['comp']}</td>
          <td>{r['match_date']}</td>
          <td>{r['home']} vs {r['away']}</td>
          <td style="font-weight:bold;font-size:1.1em">{score}</td>
          <td>{bet_cells or '<span style="color:#475569">—</span>'}</td>
        </tr>"""

    # Summary bar
    pl_color = "#16a34a" if total_pl >= 0 else "#dc2626"
    pl_str = f"+{total_pl:.2f}u" if total_pl >= 0 else f"{total_pl:.2f}u"
    win_rate = f"{wins/total_bets:.0%}" if total_bets else "—"
    summary = f"""
    <div style="padding:14px 20px;background:#0f172a;border-bottom:1px solid #334155;
                display:flex;align-items:center;gap:24px;flex-wrap:wrap">
      <span style="color:#94a3b8">{total_bets} apuestas PEV registradas</span>
      <span style="color:#94a3b8">{wins} wins / {total_bets - wins} losses ({win_rate})</span>
      <span>P&amp;L total: <strong style="color:{pl_color};font-size:1.2em">{pl_str}</strong></span>
      <span style="color:#475569;font-size:.8em">1 unidad por apuesta</span>
    </div>""" if total_bets else ""

    return summary + f"""
    <table>
      <thead><tr>
        <th>Liga</th><th>Fecha</th><th>Partido</th>
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
<meta http-equiv="refresh" content="300">
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
  td {{ padding:10px 14px; border-bottom:1px solid #1e3a5f; vertical-align:middle }}
  tr:last-child td {{ border-bottom:none }}
  tr:hover td {{ background:rgba(255,255,255,.03) }}
  .legend {{ display:flex; gap:16px; padding:14px 20px; flex-wrap:wrap;
             background:#0f172a; border-top:1px solid #1e293b; font-size:.8em;
             color:#64748b }}
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
      <h2>Apuestas con valor esperado positivo (cuota 8pm Chile)</h2>
    </div>
    <div class="section-body">
      {build_value_table(bets)}
    </div>
    <div class="legend">
      <span>EV = retorno esperado si la probabilidad de Opta es correcta</span>
      <span>·</span>
      <span>Cuotas: API Football · Odds blockeadas a las 8pm Chile (23:00 UTC) del día anterior al partido</span>
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
