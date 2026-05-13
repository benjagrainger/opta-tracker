"""Genera el dashboard HTML. Se llama automáticamente después de cada scrape."""
import sqlite3
from datetime import datetime
from pathlib import Path
from db import get_conn

OUTPUT = Path(__file__).parent / "docs" / "index.html"


def load_data():
    with get_conn() as conn:
        # Latest odds per prediction
        value_bets = conn.execute("""
            SELECT p.comp, p.home, p.away, p.match_date,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away,
                   o.impl_home, o.impl_draw, o.impl_away,
                   o.delta_home, o.delta_draw, o.delta_away,
                   o.fetched_at
            FROM predictions p
            JOIN odds o ON o.prediction_id = p.id
            WHERE o.id IN (SELECT MAX(id) FROM odds GROUP BY prediction_id)
              AND p.id NOT IN (SELECT prediction_id FROM results)
            ORDER BY p.match_date, p.home
        """).fetchall()

        results = conn.execute("""
            SELECT p.comp, p.home, p.away, p.match_date,
                   p.prob_home, p.prob_draw, p.prob_away,
                   o.odds_home, o.odds_draw, o.odds_away,
                   o.delta_home, o.delta_draw, o.delta_away,
                   r.home_score, r.away_score, r.outcome
            FROM predictions p
            JOIN odds o ON o.prediction_id = p.id
            JOIN results r ON r.prediction_id = p.id
            WHERE o.id IN (SELECT MAX(id) FROM odds GROUP BY prediction_id)
            ORDER BY p.match_date DESC
            LIMIT 100
        """).fetchall()

        stats = conn.execute("""
            SELECT COUNT(*) as total FROM predictions
        """).fetchone()

    return [dict(r) for r in value_bets], [dict(r) for r in results], dict(stats)


def delta_color(d):
    if d is None: return "#888"
    if d >= 8:  return "#16a34a"
    if d >= 4:  return "#65a30d"
    if d >= 2:  return "#ca8a04"
    if d <= -5: return "#dc2626"
    return "#64748b"

def delta_bg(d):
    if d is None: return ""
    if d >= 8:  return "background:#dcfce7"
    if d >= 4:  return "background:#f0fdf4"
    if d >= 2:  return "background:#fefce8"
    return ""

def outcome_icon(outcome, side):
    if outcome == side: return "✅"
    return "❌"

def comp_flag(comp):
    flags = {
        "LL": "🇪🇸", "EPL": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "LI1": "🇫🇷", "BUN": "🇩🇪",
        "MLS": "🇺🇸", "CHA": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "WSL": "⚽", "LEO": "🌍",
        "SPL": "🏴󠁧󠁢󠁳󠁣󠁴󠁿", "SA": "🇮🇹", "BRAS": "🇧🇷",
    }
    return flags.get(comp, "⚽")


def build_value_table(bets):
    """Builds candidate value bets grouped by match."""
    candidates = []
    for b in bets:
        for side, opta, odds, delta, impl in [
            ("L", b["prob_home"], b["odds_home"], b["delta_home"], b["impl_home"]),
            ("E", b["prob_draw"], b["odds_draw"], b["delta_draw"], b["impl_draw"]),
            ("V", b["prob_away"], b["odds_away"], b["delta_away"], b["impl_away"]),
        ]:
            if delta and delta >= 3.0:
                team = b["home"] if side=="L" else (b["away"] if side=="V" else "Empate")
                ev = (opta/100)*odds - 1
                candidates.append({**b, "side": side, "team": team,
                                    "opta": opta, "odds": odds, "delta": delta,
                                    "impl": impl, "ev": ev})
    candidates.sort(key=lambda x: x["delta"], reverse=True)

    if not candidates:
        return '<p style="color:#64748b;padding:20px">No hay bets con Δ ≥ 3pp en el ticker actual. Esperá al próximo scrape.</p>'

    rows = ""
    for c in candidates:
        ev_str = f"{c['ev']:+.0%}"
        ev_color = "#16a34a" if c["ev"] > 0 else "#dc2626"
        side_label = {"L":"Local","E":"Empate","V":"Visitante"}[c["side"]]
        rows += f"""
        <tr style="{delta_bg(c['delta'])}">
          <td>{comp_flag(c['comp'])} {c['comp']}</td>
          <td>{c['match_date']}</td>
          <td><strong>{c['home']}</strong> vs <strong>{c['away']}</strong></td>
          <td>{side_label}: <strong>{c['team']}</strong></td>
          <td style="font-size:1.1em;font-weight:bold">{c['odds']:.2f}</td>
          <td>{c['opta']:.1f}%</td>
          <td>{c['impl']:.1f}%</td>
          <td style="color:{delta_color(c['delta'])};font-weight:bold;font-size:1.1em">{c['delta']:+.1f}pp</td>
          <td style="color:{ev_color};font-weight:bold">{ev_str}</td>
        </tr>"""
    return f"""
    <table>
      <thead><tr>
        <th>Liga</th><th>Fecha</th><th>Partido</th><th>Apuesta</th>
        <th>Cuota</th><th>Opta %</th><th>Mercado %</th><th>Δ</th><th>EV</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_results_table(results):
    if not results:
        return '<p style="color:#64748b;padding:20px">Aún no hay resultados registrados.</p>'

    total = len(results)
    # Bets that had delta >= 5
    bets_won = bets_lost = 0
    for r in results:
        for side, delta in [("L",r["delta_home"]),("E",r["delta_draw"]),("V",r["delta_away"])]:
            if delta and delta >= 5:
                won = r["outcome"] == side
                if won: bets_won += 1
                else:   bets_lost += 1
    total_bets = bets_won + bets_lost
    roi_str = ""
    if total_bets > 0:
        roi_str = f"<strong>{bets_won}/{total_bets}</strong> ganadas con Δ≥5pp"

    rows = ""
    for r in results:
        score = f"{r['home_score']}-{r['away_score']}"
        outcome_map = {"H":"Local ✅","D":"Empate","A":"Visitante ✅"} if r["outcome"] else {}
        best_delta = max(
            (r["delta_home"] or -99, "L"),
            (r["delta_draw"] or -99, "E"),
            (r["delta_away"] or -99, "V"),
        )
        flag_bet = ""
        if best_delta[0] >= 5:
            team = r["home"] if best_delta[1]=="L" else (r["away"] if best_delta[1]=="V" else "Empate")
            hit = r["outcome"] == best_delta[1]
            flag_bet = f"{'✅' if hit else '❌'} {team} ({best_delta[0]:+.0f}pp)"

        rows += f"""
        <tr>
          <td>{comp_flag(r['comp'])} {r['comp']}</td>
          <td>{r['match_date']}</td>
          <td>{r['home']} vs {r['away']}</td>
          <td style="font-weight:bold;font-size:1.1em">{score}</td>
          <td>{flag_bet}</td>
        </tr>"""

    summary = f'<p style="margin-bottom:12px;color:#475569">{total} partidos | {roi_str}</p>' if roi_str else ""
    return summary + f"""
    <table>
      <thead><tr><th>Liga</th><th>Fecha</th><th>Partido</th><th>Resultado</th><th>Apuesta Opta</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def generate():
    bets, results, stats = load_data()
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    next_scrape = "8:00 / 14:00 / 20:00 hs (automático)"

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
             background:#0f172a; border-top:1px solid #1e293b; font-size:.8em }}
  .legend-item {{ display:flex; align-items:center; gap:6px; color:#94a3b8 }}
  .dot-g {{ width:10px;height:10px;border-radius:50%;background:#16a34a }}
  .dot-y {{ width:10px;height:10px;border-radius:50%;background:#ca8a04 }}
  .empty {{ padding:24px; color:#475569; font-style:italic }}
</style>
</head>
<body>
<div class="header">
  <h1>⚽ Opta Tracker</h1>
  <p>Comparativa de probabilidades Opta vs mercado de apuestas</p>
  <div class="badges">
    <span class="badge">🕐 Actualizado: <strong>{now}</strong></span>
    <span class="badge">🔄 Próximo scrape: <strong>{next_scrape}</strong></span>
    <span class="badge">📊 Partidos en DB: <strong>{stats['total']}</strong></span>
    <span class="badge">Fuente cuotas: <strong>Sofascore</strong></span>
  </div>
</div>

<div class="container">

  <div class="section">
    <div class="section-header">
      <div class="dot"></div>
      <h2>Value Bets actuales — Δ ≥ 3pp (Opta ve más que el mercado)</h2>
    </div>
    <div class="section-body">
      {build_value_table(bets)}
    </div>
    <div class="legend">
      <div class="legend-item"><div class="dot-g"></div> Δ ≥ 8pp: edge fuerte</div>
      <div class="legend-item"><div class="dot-y"></div> Δ 4–8pp: edge moderado</div>
      <div class="legend-item">EV = retorno esperado si la prob. de Opta es correcta</div>
      <div class="legend-item">Δ = Opta % − probabilidad implícita del mercado (sin margen)</div>
    </div>
  </div>

  <div class="section">
    <div class="section-header">
      <h2>📋 Historial de partidos resueltos</h2>
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
