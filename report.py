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

    picks.sort(key=lambda x: -x["ev"])
    if not picks:
        return '<p style="color:#8b949e;padding:32px 24px;font-size:.95em">No hay apuestas con ventaja en este momento. Volvé más tarde.</p>'

    side_label = {"L": "Local", "E": "Empate", "V": "Visitante"}
    cards = ""
    for p in picks:
        hora = utc_to_chile(p.get("match_time_utc"), p.get("match_date"))
        hora_str = f"{hora} hs · " if hora else ""
        team_bet = (p["home_display"] if p["side"] == "L"
                    else p["away_display"] if p["side"] == "V"
                    else "Empate")

        side_key = {"L": "home", "E": "draw", "V": "away"}[p["side"]]
        first_odds = p.get(f"first_odds_{side_key}")
        arrow = ""
        if first_odds and abs(p["odds"] - first_odds) >= 0.03:
            arrow = "↑" if p["odds"] > first_odds else "↓"
            odds_color = "#3fb950" if p["odds"] > first_odds else "#f85149"
        else:
            odds_color = "#e6edf3"

        ev_pct = f"+{p['ev']:.1%}"
        cards += f"""
        <div class="pick-card">
          <div class="pick-top">
            <span class="pick-league">{comp_flag(p['comp'])} {p['league_display']}</span>
            <span class="pick-time">{hora_str}{p['match_date']}</span>
          </div>
          <div class="pick-match">{p['home_display']}<span class="pick-vs"> vs </span>{p['away_display']}</div>
          <div class="pick-bottom">
            <div>
              <div class="pick-label">{side_label[p['side']]}</div>
              <div class="pick-team">{team_bet}</div>
              <div class="pick-opta">Opta: {p['opta']:.1f}%</div>
            </div>
            <div style="text-align:right">
              <div class="pick-odds" style="color:{odds_color}">{arrow}{p['odds']:.2f}</div>
              <div class="pick-ev">{ev_pct} ventaja</div>
            </div>
          </div>
        </div>"""
    return f'<div class="picks-grid">{cards}</div>'


def generate():
    bets, results, stats = load_data()
    now = datetime.now().strftime("%d/%m/%Y %H:%M")

    picks_html   = build_picks_cards(bets)
    analysis_html = build_value_table(bets)
    history_html  = build_results_table(results)

    n_picks = sum(
        1 for b in bets
        for side, opta, odds in [
            ("L", b["prob_home"], b["odds_home"]),
            ("E", b["prob_draw"], b["odds_draw"]),
            ("V", b["prob_away"], b["odds_away"]),
        ]
        if opta and odds and 0 < (opta / 100) * odds - 1 <= 1.0
    )
    pev_count = n_picks  # kept for badge

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
  *{{ box-sizing:border-box; margin:0; padding:0 }}
  body{{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,sans-serif;
        background:var(--bg); color:var(--text); min-height:100vh; line-height:1.5 }}

  /* ── HERO ── */
  .hero{{ padding:48px 32px 28px; max-width:1100px; margin:0 auto }}
  .hero-eyebrow{{ font-size:.72em; font-weight:700; letter-spacing:.14em;
                 color:var(--green); text-transform:uppercase; margin-bottom:10px }}
  .hero h1{{ font-size:2.2em; font-weight:800; line-height:1.15; margin-bottom:10px }}
  .hero p{{ font-size:.95em; color:var(--muted); max-width:560px; line-height:1.65 }}
  .hero-meta{{ display:flex; gap:24px; margin-top:20px; flex-wrap:wrap;
              font-size:.78em; color:var(--muted) }}
  .hero-meta span{{ display:flex; align-items:center; gap:5px }}
  .dot-live{{ width:7px; height:7px; border-radius:50%; background:var(--green);
             box-shadow:0 0 6px var(--green); animation:blink 2s infinite }}
  @keyframes blink{{ 0%,100%{{opacity:1}}50%{{opacity:.3}} }}

  /* ── CONTAINER ── */
  .wrap{{ max-width:1100px; margin:0 auto; padding:0 24px 60px }}

  /* ── SECTION LABEL ── */
  .section-label{{ font-size:.68em; font-weight:700; letter-spacing:.13em;
                  text-transform:uppercase; color:var(--muted);
                  margin:36px 0 14px; padding-bottom:8px;
                  border-bottom:1px solid var(--border2) }}

  /* ── PICK CARDS ── */
  .picks-grid{{ display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:12px }}
  .pick-card{{
    background:var(--surface); border:1px solid var(--border);
    border-radius:10px; padding:18px 20px; cursor:default;
    transition:border-color .15s, transform .1s;
  }}
  .pick-card:hover{{ border-color:var(--green); transform:translateY(-1px) }}
  .pick-top{{ display:flex; justify-content:space-between; align-items:center;
             font-size:.72em; color:var(--muted); margin-bottom:10px }}
  .pick-match{{ font-size:1em; font-weight:600; margin-bottom:16px; line-height:1.35 }}
  .pick-match .pick-vs{{ color:var(--muted); font-weight:400; font-size:.88em }}
  .pick-bottom{{ display:flex; justify-content:space-between; align-items:flex-end }}
  .pick-label{{ font-size:.7em; text-transform:uppercase; letter-spacing:.08em;
               color:var(--muted); margin-bottom:2px }}
  .pick-team{{ font-size:.95em; font-weight:600; color:var(--text) }}
  .pick-opta{{ font-size:.72em; color:var(--dim); margin-top:3px }}
  .pick-odds{{ font-size:1.9em; font-weight:800; line-height:1 }}
  .pick-ev{{ font-size:.82em; font-weight:700; color:var(--green); margin-top:3px }}

  /* ── FULL ANALYSIS TABLE ── */
  .table-wrap{{ border-radius:8px; border:1px solid var(--border); overflow:hidden; overflow-x:auto }}
  table{{ width:100%; border-collapse:collapse; font-size:.85em }}
  th{{ background:var(--surface); color:var(--muted); font-weight:600;
      text-transform:uppercase; font-size:.68em; letter-spacing:.08em;
      padding:10px 16px; text-align:left; border-bottom:1px solid var(--border) }}
  td{{ padding:11px 16px; border-bottom:1px solid var(--border2);
      color:var(--text); vertical-align:middle }}
  tr:last-child td{{ border-bottom:none }}
  tr:hover td{{ background:rgba(255,255,255,.025) }}

  /* ── LEGEND ── */
  .legend{{ display:flex; gap:16px; padding:12px 16px; flex-wrap:wrap;
           font-size:.75em; color:var(--muted); background:var(--surface);
           border-top:1px solid var(--border2) }}

  /* ── HISTORY SUMMARY ── */
  .hist-summary{{ padding:16px 20px; background:var(--surface);
                 display:flex; gap:28px; flex-wrap:wrap; align-items:center;
                 border-bottom:1px solid var(--border) }}
</style>
</head>
<body>

<div class="hero">
  <div class="hero-eyebrow">Opta Value Bets</div>
  <h1>Apostá con ventaja<br>sobre la casa</h1>
  <p>Opta es el modelo estadístico que usan los propios bookmakers. Cuando sus cuotas pagan más de lo que Opta calcula, hay una ventaja real para vos.</p>
  <div class="hero-meta">
    <span><span class="dot-live"></span> En vivo · {now}</span>
    <span>📊 {stats['total']} partidos analizados</span>
    <span style="color:{'var(--green)' if pev_count else 'var(--muted)'}">
      🎯 {pev_count} apuesta{'s' if pev_count != 1 else ''} con ventaja ahora
    </span>
  </div>
</div>

<div class="wrap">

  <div class="section-label">Apuestas recomendadas</div>
  {picks_html}

  <div class="section-label">Análisis completo · todos los resultados</div>
  <div class="table-wrap">
    {analysis_html}
    <div class="legend">
      <span>★ = apuesta recomendada del partido</span>
      <span>·</span>
      <span><span style="color:var(--green)">↑</span> cuota subió desde la primera detección (mejor para vos)</span>
      <span>·</span>
      <span><span style="color:var(--red)">↓</span> cuota bajó (mercado ya lo corrigió)</span>
    </div>
  </div>

  <div class="section-label">Historial de apuestas</div>
  <div class="table-wrap">
    {history_html}
  </div>

</div>
</body>
</html>"""

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"  Dashboard generado → {OUTPUT}")


if __name__ == "__main__":
    generate()
