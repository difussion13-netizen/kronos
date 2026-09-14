#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""clobzero.py — три оценки по датасету окон clobwin: экономика MM / нулевая гипотеза / миспрайсинг.

  E1 (econ):   спреды, поток, пай тейкер-фи — «есть ли что собирать»
  E2 (sim):    пассивный двусторонний котинг ±q¢ по чекпоинтам, исход по нашим минуткам
  E3 (calib):  калибровка цены рынка vs realized freq (миспрайсинг) и Brier против якоря

Вход: outdir clobwin (windows_*.csv, minutes_*.csv) + датасет dataset.py (для volat12 якоря).
Гейты — см. docs/plan-4-evals.md. Это симуляция ПРИМЛЕНИЯ по чекпоинтам: P&L E2 — верхняя
граница (не учитывает очередь, частичные исполнения, задержку снятия).
"""
import argparse
import bisect
import csv
import glob
import json
import math
import os

Q_CENTS = 0.01


def load_windows(win_dir, assets, codes):
    rows = []
    for path in sorted(glob.glob(os.path.join(win_dir, "windows_*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                if r["asset"] not in assets or r["code"] not in codes:
                    continue
                rows.append(r)
    return rows


def load_minutes(win_dir):
    per = {}
    for path in sorted(glob.glob(os.path.join(win_dir, "minutes_*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                d = per.setdefault(r["asset"], {"m": [], "c": []})
                d["m"].append(int(r["minute"])); d["c"].append(float(r["close"]))
    for d in per.values():
        order = sorted(range(len(d["m"])), key=lambda i: d["m"][i])
        d["m"] = [d["m"][i] for i in order]; d["c"] = [d["c"][i] for i in order]
    return per


def mget(per, asset, ts):
    """close последней минуты <= ts (carry forward)."""
    d = per.get(asset)
    if not d:
        return None
    i = bisect.bisect_right(d["m"], ts) - 1
    return d["c"][i] if i >= 0 else None


def load_bars(ds_dir, assets):
    """asset -> (t0 список, volat12 список) из 5м-датасета."""
    out = {}
    for a in assets:
        p = os.path.join(ds_dir, f"{a}_300s.csv")
        if not os.path.exists(p):
            continue
        ts, vv = [], []
        with open(p) as f:
            for r in csv.DictReader(f):
                ts.append(int(float(r["t0"])))
                vv.append(max(0.5, float(r.get("volat12") or 8.0)))
        out[a] = (ts, vv)
    return out


def bar_vol(bars, asset, t):
    d = bars.get(asset)
    if not d:
        return 8.0
    i = bisect.bisect_right(d[0], t) - 1
    return d[1][i] if i >= 0 else 8.0


def phi(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def slot_mid(r, c):
    b = r.get(f"b{c}"); a = r.get(f"a{c}")
    if b in (None, "") or a in (None, ""):
        return None, None, None
    b = float(b); a = float(a)
    return (b + a) / 2.0, b, a


def cp_list(code):
    if code == "5m":
        return [240, 120, 60, 30, 15, 5, 0]
    if code == "15m":
        return [600, 240, 120, 60, 30, 15, 5, 0]
    return [1800, 600, 240, 120, 60, 30, 15, 5, 0]


def econ(rows):
    print("== E1: экономика книг (по кодам/активам) ==")
    agg = {}
    for r in rows:
        k = (r["asset"], r["code"])
        d = agg.setdefault(k, {"n": 0, "sp": [], "lt": [], "vol": []})
        d["n"] += 1
        m, b, a = slot_mid(r, 120)
        if a is not None and b is not None:
            d["sp"].append(a - b)
        d["lt"].append(int(float(r["n_lt"])))
        d["vol"].append(float(r["vol_usd"]))
    out = {}
    for k, d in sorted(agg.items()):
        def q(v, p):
            v = sorted(v);  return v[min(len(v) - 1, int(p * len(v)))] if v else 0
        pool = 0.0625 * 0.25 * sum(d["vol"])                 # taker-fee пай при p≈0.5
        rec = dict(windows=d["n"], spread_p50_c=round(100 * q(d["sp"], .5), 2),
                   spread_p90_c=round(100 * q(d["sp"], .9), 2),
                   trades_p50=q(d["lt"], .5), vol_usd_day=round(sum(d["vol"])),
                   fee_pool_usd_day=round(pool), maker_pool20_usd_day=round(0.2 * pool))
        out["%s/%s" % k] = rec
        print(f"  {k[0]:<4}{k[1]:<4}", {kk: vv for kk, vv in rec.items()})
    return out


def calib(rows, per, bars, quote=False):
    print("\n== E3: калибровка/миспрайсинг (цена up-токена vs realized freq; якорь — только информация до чекпоинта) ==")
    res = {}
    for c in [240, 120, 60, 30, 15]:
        bins = {}
        bm = ba = n = 0
        byday = {}
        for r in rows:
            asset = r["asset"]; start = int(r["start"]); end = int(r["end"])
            m, _, _ = slot_mid(r, c)
            if m is None:
                continue
            bench = mget(per, asset, start)
            fin = mget(per, asset, end - 1)
            if bench is None or fin is None:
                continue
            y = 1 if (fin >= bench) else 0                    # тай -> Up (правило площадки)
            b = int(min(9, max(0, m * 10)))
            d = bins.setdefault(b, [0, 0])
            d[0] += 1; d[1] += y
            pm = min(max(m, 1e-4), 1 - 1e-4)
            bm += (pm - y) ** 2; n += 1
            # честный якорь: информация СТРОГО до чекпоинта — последняя fully
            # closed минута и последняя полностью закрытая 5м-палка (иначе
            # close минуты «заглядывает» на +45..55 с, а volat12 бара — на +300 с)
            spot = mget(per, asset, end - c - 60) or bench
            d_bps = (spot / bench - 1.0) * 1e4
            sig = max(0.5, bar_vol(bars, asset, end - c - 301))
            pa = phi(d_bps / (sig * math.sqrt(max(10.0, c) / 300.0)))
            pa = min(max(pa, 1e-4), 1 - 1e-4)
            ba += (pa - y) ** 2
            dd = byday.setdefault(start // 86400, [0.0, 0.0, 0])
            dd[0] += (pm - y) ** 2; dd[1] += (pa - y) ** 2; dd[2] += 1
        if not n:
            print(f"  cp={c:>3}: нет данных")
            continue
        ece = 0.0
        print(f"  cp={c:>3} n={n:5d}  Brier market {bm/n:.4f}  Brier anchor {ba/n:.4f}"
              f"  (климатол. {0.25:.4f})")
        for b in sorted(bins):
            cnt, ups = bins[b]
            if cnt >= 8:
                p = (b + 0.5) / 10.0
                ece += cnt / n * abs(ups / cnt - p)
        win = sum(1 for eb, ea, nn in byday.values() if nn >= 30 and ea / nn < eb / nn)
        tot_d = sum(1 for _, _, nn in byday.values() if nn >= 30)
        print(f"        якорь лучше рынка: {win}/{tot_d} дней (n≥30); "
              f"ECE {ece*100:.2f} ¢; бакеты (p_mid → realized):")
        line = "        "
        for b in sorted(bins):
            cnt, ups = bins[b]
            line += f"[{(b+0.5)/10:.1f}→{ups/cnt:.2f} n{cnt}] "
        print(line)
        res[f"cp{c}"] = dict(n=n, brier_m=round(bm/n, 4), brier_a=round(ba/n, 4),
                             ece=round(ece, 4), anchor_beats_days=f"{win}/{tot_d}")
    return res


def sim(rows, per, quote_c=Q_CENTS, no_last=True):
    print("\n== E2: нулевая MM-гипотеза (двусторонний BUY: YES по mid\u2212q, NO по 1\u2212(mid+q)) ==")
    tot = {"pair": 0, "single": 0, "pnl": 0.0, "rebate": 0.0, "mark": [], "day": {}}
    for r in rows:
        asset = r["asset"]; start = int(r["start"]); end = int(r["end"])
        mids = {}
        for c in cp_list(r["code"]):
            m, _, _ = slot_mid(r, c)
            if m is not None:
                mids[c] = m
        if len(mids) < 2:
            continue
        bench = mget(per, asset, start); fin = mget(per, asset, end - 1)
        if bench is None or fin is None:
            continue
        y = 1 if fin >= bench else 0
        quote_cps = [c for c in (240, 120, 60, 30) if c in mids]
        if no_last:
            quote_cps = [c for c in quote_cps if c >= 60]      # «пауза» в последние ~90 с
        if len(quote_cps) < 2:
            continue
        pos = 0   # +1 держим YES (по price), -1 держим NO
        px = 0.0
        pnl = 0.0
        for i in range(len(quote_cps) - 1):
            c1, c2 = quote_cps[i], quote_cps[i + 1]
            bid = max(0.005, mids[c1] - quote_c)
            ask = min(0.995, mids[c1] + quote_c)
            if pos == 0:
                if mids[c2] <= bid:
                    pos, px = 1, bid
                elif mids[c2] >= ask:
                    pos, px = -1, 1 - ask
            elif pos == 1 and mids[c2] >= ask:      # купили NO -> пара (merge)
                tot["pair"] += 1
                prof = (ask - bid) * 100 + reb(bid) + reb(1 - ask)
                tot["pnl"] += prof; tot["rebate"] += reb(bid) + reb(1 - ask)
                pos = 0; continue
            elif pos == -1 and mids[c2] <= bid:     # купили YES -> пара
                tot["pair"] += 1
                prof = (ask - bid) * 100 + reb(1 - px) + reb(bid)
                tot["pnl"] += prof; tot["rebate"] += reb(1 - px) + reb(bid)
                pos = 0; continue
            if pos != 0:
                break                               # одна позиция максимум, дальше держим до резолва
        if pos != 0:
            tot["single"] += 1
            val = y if pos == 1 else (1 - y)
            p_ = px if pos == 1 else 1 - px
            prof = (val - px) * 100 if pos == 1 else (val - px) * 100
            prof = (val * 100 - px * 100) + reb(p_)
            tot["pnl"] += prof; tot["rebate"] += reb(p_)
            tot["mark"].append((val - px) * 100)
            day = str(start // 86400)
            dd = tot["day"].setdefault(day, [0.0, 0])
            dd[0] += prof; dd[1] += 1
    def qmed(v, p=0.5):
        v = sorted(v)
        return round(v[int(p * (len(v) - 1))], 2) if v else 0
    days = list(tot["day"].values())
    good = sum(1 for d in days if d[0] > 0)
    print(f"  парные закрытия: {tot['pair']}, одиночных до резолва: {tot['single']}")
    print(f"  P&L суммарно: ${tot['pnl']:.1f}  (в т.ч. рибейт ${tot['rebate']:.1f})")
    print(f"  медиана markout одиночных: {qmed(tot['mark'])} \u00a2, p90 {qmed(tot['mark'], .9)} \u00a2")
    print(f"  дней с P&L>0: {good}/{len(days)}; медианный P&L/день {qmed([d[0] for d in days])}$")
    ok = days and good >= 0.7 * len(days) and tot["pnl"] > 0 and qmed(tot["mark"]) > -0.5
    print("  вердикт:", "гейт E2 ПРОЙДЕН — строить MM-слой" if ok else "гейт E2 НЕ пройден — пассив на этих книгах = донор")
    return dict(pair=tot["pair"], single=tot["single"], pnl=round(tot["pnl"], 2),
                rebate=round(tot["rebate"], 2), days_pos=good, days=len(days),
                markout_med=qmed(tot["mark"]), markout_p90=qmed(tot["mark"], .9))


def reb(p):
    return 0.2 * 0.0625 * p * (1 - p) * 100.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--win", default="/tmp/win")
    ap.add_argument("--ds", default="/tmp/ds")
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    ap.add_argument("--codes", default="5m,15m")
    ap.add_argument("--quote-c", type=float, default=1.0, help="¢ в обе стороны")
    ap.add_argument("--mode", choices=["all", "econ", "calib", "sim"], default="all")
    a = ap.parse_args()
    assets = set(x.strip() for x in a.assets.split(","))
    codes = set(x.strip() for x in a.codes.split(","))
    rows = load_windows(a.win, assets, codes)
    print(f"# clobzero: окон {len(rows)} (активы {sorted(assets)}, коды {sorted(codes)})")
    if not rows:
        raise SystemExit("нет окон — сначала clobwin.py")
    per = load_minutes(a.win)
    bars = load_bars(a.ds, assets)
    out = {}
    if a.mode in ("all", "econ"):
        out["econ"] = econ(rows)
    if a.mode in ("all", "calib"):
        out["calib"] = calib(rows, per, bars)
    if a.mode in ("all", "sim"):
        out["sim"] = sim(rows, per, a.quote_c / 100.0)
        out["sim_pause"] = sim(rows, per, a.quote_c / 100.0, no_last=True) if a.mode == "all" else None
    jp = os.path.join(a.win, "eval_summary.json")
    json.dump({k: v for k, v in out.items() if v}, open(jp, "w"), ensure_ascii=False, indent=1)
    print("\nсводка ->", jp)


if __name__ == "__main__":
    main()
