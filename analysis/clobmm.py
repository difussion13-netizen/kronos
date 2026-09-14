#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""clobmm.py — MM-фаза: бумажный симулятор двустороннего котинга со скью якоря.

Гейты и модель зафиксированы ЗАРАНЕЕ в docs/plan-mm.md. Данные: windows_*.csv и
minutes_*.csv из outdir clobwin (s3://…/kronos/win) — новых тяжёлых прогонов не
нужно. P&L — ВЕРХНЯЯ граница (очередей/глубины нет), пессимизм полем f: цена
должна пересечь наш уровень на >f. Информация якоря строго до решения (урок E3):
минутные close ≤ t−60с, σ-палки ≤ t−300с. Резолв-ярлык = прокси fin≥bench по
нашим минуткам (тай → Up), как в clobzero/E2. Торгового кода нет.

    python3 clobmm.py --win /tmp/win [--ds /tmp/ds] [--no-sweep]

Гейты (первичная конфигурация k=2.5, f=0.5¢, anchor on):
  M1 ≥70% дней P&L>0 и медиана дневного P&L>0;  M2 медиана markout синглов > −0.5¢;
  M3 P&L(anchor)−P&L(neutral) > 0 на ≥2/3 дней;  M4 P&L>0 в ≥2/3 сетки k и ≥2/3 сетки f.
"""
import argparse
import bisect
import csv
import glob
import json
import math
import os

SQ2PI = math.sqrt(2.0 * math.pi)
REB = 0.2 * 0.0625


def phi(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def pdf(x):
    return math.exp(-0.5 * x * x) / SQ2PI


def reb(p):
    return REB * min(max(p, 0.0), 1.0) * (1.0 - min(max(p, 0.0), 1.0)) * 100.0


def cp_list(code):
    if code == "5m":
        return [240, 120, 60, 30, 15, 5, 0]
    if code == "15m":
        return [600, 240, 120, 60, 30, 15, 5, 0]
    return [1800, 600, 240, 120, 60, 30, 15, 5, 0]


def load_windows(win_dir, assets, codes):
    rows = []
    for path in sorted(glob.glob(os.path.join(win_dir, "windows_*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                if r["asset"] in assets and r["code"] in codes:
                    rows.append(r)
    rows.sort(key=lambda r: r["start"])
    return rows


def load_minutes(win_dir):
    per = {}
    for path in sorted(glob.glob(os.path.join(win_dir, "minutes_*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                d = per.setdefault(r["asset"], {"m": [], "c": []})
                d["m"].append(int(r["minute"])); d["c"].append(float(r["close"]))
    for d in per.values():
        o = sorted(range(len(d["m"])), key=lambda i: d["m"][i])
        d["m"] = [d["m"][i] for i in o]; d["c"] = [d["c"][i] for i in o]
        n = len(d["m"])
        cs = [0.0] * n; c2 = [0.0] * n; vc = [0] * n   # префиксы ПО ПОЗИЦИЯМ m,
        for i in range(1, n):                            # дырявые минуты не сдвигают
            cs[i] = cs[i - 1]; c2[i] = c2[i - 1]; vc[i] = vc[i - 1]
            if d["m"][i] - d["m"][i - 1] <= 120 and d["c"][i - 1] > 0:
                r = math.log(d["c"][i] / d["c"][i - 1]) * 1e4
                cs[i] += r; c2[i] += r * r; vc[i] += 1
        d["cs"], d["c2"], d["vc"] = cs, c2, vc
    return per


def mget(per, asset, ts):
    d = per.get(asset)
    if not d:
        return None
    i = bisect.bisect_right(d["m"], ts) - 1
    return d["c"][i] if i >= 0 else None


def load_bars(ds_dir, assets):
    out = {}
    if not ds_dir or not os.path.isdir(ds_dir):
        return out
    for a in assets:
        p = os.path.join(ds_dir, f"{a}_300s.csv")
        if os.path.exists(p):
            ts, vv = [], []
            with open(p) as f:
                for r in csv.DictReader(f):
                    ts.append(int(float(r["t0"])))
                    vv.append(max(0.5, float(r.get("volat12") or 8.0)))
            out[a] = (ts, vv)
    return out


class Sigma:
    """σ б.п./5м-бар строго до t: volat12 датасета (t0+300 ≤ t) или std 120 мин."""

    def __init__(self, per, bars):
        self.per, self.bars, self.memo = per, bars, {}

    def _from_minutes(self, asset, t):
        d = self.per.get(asset)
        if not d or len(d["m"]) < 40:
            return 0.0
        j = bisect.bisect_right(d["m"], t) - 1
        lo = max(0, j - 120)
        n = d["vc"][j] - d["vc"][lo]
        if n < 30:
            return 0.0
        s = d["cs"][j] - d["cs"][lo]; s2 = d["c2"][j] - d["c2"][lo]
        return math.sqrt(max(0.0, s2 / n - (s / n) ** 2)) * math.sqrt(5.0)

    def get(self, asset, t):
        key = (asset, t // 60)
        v = self.memo.get(key)
        if v is not None:
            return v
        v = 0.0
        bd = self.bars.get(asset)
        if bd:
            i = bisect.bisect_right(bd[0], t - 301) - 1
            if i >= 0:
                v = bd[1][i]
        if v <= 0:
            v = self._from_minutes(asset, t)
        v = max(2.0, v)
        self.memo[key] = v
        return v


def qmed(v, p=0.5):
    v = sorted(v)
    return round(v[int(p * (len(v) - 1))], 2) if v else 0.0


def sim(rows, per, sig, anchor, k, f):
    tot = {"pnl": 0.0, "fills": 0, "pairs": 0, "singles": 0, "skip": 0}
    marks, marks_x = [], []
    days = {}

    def add(x, start):
        tot["pnl"] += x
        d = start // 86400
        days[d] = days.get(d, 0.0) + x

    for r in rows:
        asset = r["asset"]; start = int(r["start"]); end = int(r["end"]); code = r["code"]
        bench = mget(per, asset, start); fin = mget(per, asset, end - 1)
        if bench is None or fin is None or bench <= 0:
            tot["skip"] += 1
            continue
        y = 1 if fin >= bench else 0
        mids = {}
        for c in cp_list(code):
            b = r.get("b%d" % c); a = r.get("a%d" % c)
            if b not in (None, "") and a not in (None, ""):
                mids[c] = (float(b) + float(a)) / 2.0
        cps = cp_list(code)
        pos, px = 0, 0.0
        for c in [x for x in cps if x >= 60]:
            i = cps.index(c)
            c2 = cps[i + 1] if i + 1 < len(cps) else None
            mid2 = mids.get(c2) if c2 is not None else None
            if mid2 is None:
                continue
            t1 = end - c
            spot = mget(per, asset, t1 - 60) or bench
            d_bps = (spot / bench - 1.0) * 1e4
            tau = float(max(10, c))
            s5 = sig.get(asset, t1 - 60)
            x = d_bps / (s5 * math.sqrt(tau / 300.0))
            pa = min(max(phi(x), 0.01), 0.99)
            center = pa if anchor else mids.get(c, pa)
            step = float(max(15, min(c - c2, int(tau))))
            h = max(0.005, k * pdf(x) * math.sqrt(step / tau))
            bid = min(max(center - h, 0.005), 0.99)
            ask = min(max(bid + 0.01, center + h), 0.995)
            eb = bid - mid2 if (pos <= 0 and mid2 <= bid - f) else -1.0
            ea = mid2 - ask if (pos >= 0 and mid2 >= ask + f) else -1.0
            if eb < 0 and ea < 0:
                continue
            if ea > eb:                                   # спросили наш ASK (продали YES)
                if pos == 1:                             # выход по YES = пара
                    g = (ask - px) * 100.0
                    add(g + reb(ask), start); marks_x.append(g)
                    tot["pairs"] += 1; pos = 0
                elif pos == 0:                            # без инвентаря = покупка NO по 1-ask
                    pos, px = -1, 1.0 - ask; tot["fills"] += 1
                    add(reb(1.0 - ask), start)
            else:                                         # наш BID забрали (купили YES)
                if pos == -1:                             # YES+NO = $1, закрытие пары
                    g = (1.0 - bid - px) * 100.0
                    add(g + reb(bid), start); marks_x.append(g)
                    tot["pairs"] += 1; pos = 0
                elif pos == 0:
                    pos, px = 1, bid; tot["fills"] += 1
                    add(reb(bid), start)
        if pos != 0:
            val = y if pos == 1 else 1 - y
            g = (val - px) * 100.0 + reb(px)
            add(g, start); marks.append(g); tot["singles"] += 1
    dv = list(days.values())
    return dict(pnl=round(tot["pnl"], 1), fills=tot["fills"], pairs=tot["pairs"],
                singles=tot["singles"], skip=tot["skip"],
                mark_s_med=qmed(marks), mark_s_p90=qmed(marks, .9),
                mark_x_med=qmed(marks_x),
                days_n=len(dv), days_pos=sum(1 for x in dv if x > 0),
                day_med=qmed(dv), days=days)


def fmt(tag, m):
    return (f"{tag:<26} fills {m['fills']:>5}  pairs {m['pairs']:>4}  singl {m['singles']:>5}"
            f"  P&L {m['pnl']:>12.1f}$  дн+ {m['days_pos']:>2}/{m['days_n']:<2}"
            f"  markout_med {m['mark_s_med']:>6.2f}¢  p90 {m['mark_s_p90']:>5.1f}¢")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--win", default="/tmp/win")
    ap.add_argument("--ds", default="/tmp/ds", help="датасеты dataset.py (для volat12); нет — σ из минуток")
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    ap.add_argument("--codes", default="5m,15m")
    ap.add_argument("--k", type=float, default=2.5, help="множитель полуширины")
    ap.add_argument("--fill-margin", type=float, default=0.5, help="¢ пессимизма касания")
    ap.add_argument("--no-sweep", action="store_true")
    a = ap.parse_args()
    assets = set(x.strip() for x in a.assets.split(","))
    codes = set(x.strip() for x in a.codes.split(","))
    rows = load_windows(a.win, assets, codes)
    print(f"# clobmm: окон {len(rows)} (активы {sorted(assets)}, коды {sorted(codes)}); "
          f"k={a.k}, f={a.fill_margin}¢")
    if not rows:
        raise SystemExit("нет windows_*.csv — см. docs/plan-mm.md «Запуск»")
    per = load_minutes(a.win)
    sig = Sigma(per, load_bars(a.ds, assets))
    print(f"  источник σ: {'dataset volat12' if sig.bars else 'std 120 мин (fallback)'}")
    f0 = a.fill_margin / 100.0
    on = sim(rows, per, sig, True, a.k, f0)
    neu = sim(rows, per, sig, False, a.k, f0)
    print()
    print(fmt("primary (anchor on)", on))
    print(fmt("контроль (нейтр. центр)", neu))
    dd = [on["days"].get(d, 0.0) - neu["days"].get(d, 0.0) for d in sorted(on["days"])]
    wins = sum(1 for x in dd if x > 0)
    print(f"  разница по дням (on−нейтр): {wins}/{len(dd)} дней в плюс; "
          f"медиана {qmed(dd)}$")
    m1 = on["days_n"] and on["days_pos"] >= 0.7 * on["days_n"] and on["day_med"] > 0
    m2 = on["mark_s_med"] > -0.5
    m3 = wins >= max(1, -(-2 * len(dd) // 3))
    print("\n== сетка робастности (P&L, $; первичная жирным не выделена — см. выше) ==")
    ks = [1.5, a.k, 4.0]; fs = [0.0, f0, 0.01]
    grid = {}
    for kk in ks:
        for ff in fs:
            r = sim(rows, per, sig, True, kk, ff)
            grid[(kk, ff)] = r["pnl"]
            print(f"  k={kk:>4} f={100*ff:>5.1f}¢: P&L {r['pnl']:>12.1f}$  "
                  f"days+ {r['days_pos']:>2}/{r['days_n']:<2}  markout {r['mark_s_med']:>6.2f}¢")
    ok_k = sum(1 for kk in ks if grid.get((kk, f0), -1.0) > 0)
    ok_f = sum(1 for ff in fs if grid.get((a.k, ff), -1) > 0)
    m4 = ok_k >= 2 and ok_f >= 2
    res = [("M1 дни+ и медиана P&L > 0", m1),
           ("M2 markout синглов > -0.5¢", m2),
           ("M3 якорь > нейтр. контроля 2/3 дней", m3),
           ("M4 робастность сетки k и f", m4)]
    print("\n== вердикты гейтов MM-фазы ==")
    for name, ok in res:
        print(f"  {'ДА ' if ok else 'НЕТ'}  {name}")
    ny = sum(1 for _, ok in res if not ok)
    print("ИТОГ:", "фаза ММ подтверждена — следующий артефакт: модель очереди "
          "+ полные сутки (см. план)" if ny == 0 else
          f"{ny} «нет» — " + ("направление закрываем с протоколом (2+)" if ny >= 2
          else "граничный результат: расширить период данных и повторить (1)"))
    cut = lambda m: {kk: vv for kk, vv in m.items() if kk != "days"}
    out = dict(primary=cut(on), neutral=cut(neu), diff_days=f"{wins}/{len(dd)}",
               grid={f"k{kk}_f{round(100*ff,1)}c": round(grid[(kk, ff)], 1)
                     for kk in ks for ff in fs},
               gates={n: bool(o) for n, o in res})
    jp = os.path.join(a.win, "mm_summary.json")
    json.dump(out, open(jp, "w"), ensure_ascii=False, indent=1)
    print("сводка ->", jp)


if __name__ == "__main__":
    main()
