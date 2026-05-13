"""Calibration, Brier Score and ROI analysis on accumulated data."""
import sqlite3
import math
from db import get_conn


def fetch_resolved() -> list:
    """Matches with both odds and result."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
                p.comp, p.home, p.away, p.match_date,
                p.prob_home, p.prob_draw, p.prob_away,
                o.odds_home, o.odds_draw, o.odds_away,
                o.impl_home, o.impl_draw, o.impl_away,
                o.delta_home, o.delta_draw, o.delta_away,
                r.outcome
            FROM predictions p
            JOIN odds o ON o.prediction_id = p.id
            JOIN results r ON r.prediction_id = p.id
        """).fetchall()
    return [dict(r) for r in rows]


def brier_score(rows: list) -> dict:
    """Brier score for Opta and for the market."""
    def outcome_vec(outcome):
        return {"H": (1,0,0), "D": (0,1,0), "A": (0,0,1)}[outcome]

    bs_opta = bs_mkt = 0.0
    n = len(rows)
    for r in rows:
        oh, od, oa = outcome_vec(r["outcome"])
        bs_opta += (r["prob_home"]/100 - oh)**2 + (r["prob_draw"]/100 - od)**2 + (r["prob_away"]/100 - oa)**2
        bs_mkt  += (r["impl_home"]/100 - oh)**2 + (r["impl_draw"]/100 - od)**2 + (r["impl_away"]/100 - oa)**2

    return {
        "n": n,
        "brier_opta": round(bs_opta / n, 4),
        "brier_market": round(bs_mkt / n, 4),
        "brier_skill_score": round(1 - (bs_opta / n) / (bs_mkt / n), 4),
    }


def calibration(rows: list, buckets: int = 10) -> list:
    """Group predictions by probability bucket and compare to actual frequency."""
    step = 100 / buckets
    bucket_data = {i: {"predicted": [], "outcomes": []} for i in range(buckets)}

    for r in rows:
        for prob, key in [
            (r["prob_home"], "H"),
            (r["prob_draw"], "D"),
            (r["prob_away"], "A"),
        ]:
            b = min(int(prob // step), buckets - 1)
            bucket_data[b]["predicted"].append(prob / 100)
            bucket_data[b]["outcomes"].append(1 if r["outcome"] == key else 0)

    result = []
    for i, d in bucket_data.items():
        if not d["predicted"]:
            continue
        avg_pred = sum(d["predicted"]) / len(d["predicted"])
        avg_actual = sum(d["outcomes"]) / len(d["outcomes"])
        result.append({
            "bucket": f"{i*step:.0f}-{(i+1)*step:.0f}%",
            "n": len(d["predicted"]),
            "avg_predicted": round(avg_pred * 100, 1),
            "actual_freq": round(avg_actual * 100, 1),
            "diff": round((avg_actual - avg_pred) * 100, 1),
        })
    return result


def roi_by_delta(rows: list, min_delta: float = 5.0) -> dict:
    """
    Simulate flat betting 1 unit on every outcome where delta >= min_delta.
    Returns ROI stats.
    """
    bets = []
    for r in rows:
        for side, prob, odds, delta in [
            ("H", r["prob_home"], r["odds_home"], r["delta_home"]),
            ("D", r["prob_draw"], r["odds_draw"], r["delta_draw"]),
            ("A", r["prob_away"], r["odds_away"], r["delta_away"]),
        ]:
            if delta >= min_delta:
                won = (side == r["outcome"])
                profit = (odds - 1) if won else -1
                bets.append({
                    "match": f"{r['home']} vs {r['away']}",
                    "comp": r["comp"],
                    "side": side,
                    "delta": delta,
                    "odds": odds,
                    "won": won,
                    "profit": profit,
                })

    if not bets:
        return {"n_bets": 0, "roi": None, "message": f"No bets with delta >= {min_delta}pp yet"}

    total_stake = len(bets)
    total_profit = sum(b["profit"] for b in bets)
    roi = total_profit / total_stake * 100
    win_rate = sum(1 for b in bets if b["won"]) / len(bets) * 100

    return {
        "n_bets": len(bets),
        "win_rate": round(win_rate, 1),
        "total_profit": round(total_profit, 2),
        "roi": round(roi, 2),
        "min_delta": min_delta,
    }


def print_report():
    rows = fetch_resolved()
    if not rows:
        print("Aún no hay partidos resueltos. Volvé después del primer fin de semana.")
        return

    print(f"\n{'='*55}")
    print(f"  REPORTE OPTA TRACKER — {len(rows)} partidos resueltos")
    print(f"{'='*55}")

    bs = brier_score(rows)
    print(f"\nBRIER SCORE (menor = mejor)")
    print(f"  Opta:    {bs['brier_opta']}  (referencia mercado: {bs['brier_market']})")
    print(f"  Skill Score vs mercado: {bs['brier_skill_score']:+.4f}  "
          f"({'✓ Opta supera al mercado' if bs['brier_skill_score'] > 0 else '✗ Mercado supera a Opta'})")

    print(f"\nCALIBRACIÓN OPTA")
    print(f"  {'Bucket':12} {'N':>5} {'Predicho%':>10} {'Real%':>8} {'Diff':>7}")
    for b in calibration(rows):
        sign = "↑" if b["diff"] > 2 else ("↓" if b["diff"] < -2 else " ")
        print(f"  {b['bucket']:12} {b['n']:>5} {b['avg_predicted']:>10.1f} "
              f"{b['actual_freq']:>8.1f} {b['diff']:>+7.1f} {sign}")

    print(f"\nROI SIMULADO (apuesta plana 1u)")
    for threshold in [3.0, 5.0, 8.0, 10.0]:
        r = roi_by_delta(rows, threshold)
        if r["n_bets"] == 0:
            print(f"  Delta ≥ {threshold:4.1f}pp → sin apuestas aún")
        else:
            print(f"  Delta ≥ {threshold:4.1f}pp → {r['n_bets']:3} apuestas | "
                  f"Win rate {r['win_rate']:5.1f}% | ROI {r['roi']:+.1f}%")
    print()


if __name__ == "__main__":
    print_report()
