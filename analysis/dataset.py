#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dataset.py — собрать датасет для модели из сырых логов kronolog.

Что происходит: сырые тики Binance (aggTrade) из S3 -> 1-минутные бары -> бары
выбранного ТФ -> таблица фичей (только ПРОШЕДШЕЕ время!) + метки «закроется ли
рынок выше/ниже/как» через N минут. CSV (+npz, если есть numpy) в --outdir,
рядом meta.json с описаниями колонок — чтобы через полгода не гадать.

    python3 dataset.py --bucket M --days 20260903-20260909 --tf 300 \
        --assets btc,eth,sol,xrp --outdir /tmp/ds

Дисциплина фичей: все предикторы считаются по барам c ts <= закрытие текущего
бара. Метки — по будущим ценам (это нормально: они только в y).

Метка «flat» определяется порогом --floor-bps (по умолчанию 4.7 — замеренный
шум резолва Chainlink, см. docs/measured-oracle-latency.md): |движение| <= порога
считается «не было движения» — на нём торговать нельзя в принципе.
"""
import argparse
import csv
import json
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kreader as K  # noqa: E402

VERSION = "1.0"


def collect(a, assets):
    """Один проход по binance/ и rtds/ за все дни -> {asset: [(ts,px,vol)]}, {asset: [(ts,px,recv)]}."""
    tb = {x: [] for x in assets}
    clp = {x: [] for x in assets}
    base_of = {K.to_binance_sym(x): x for x in assets}            # btcusdt -> btc
    cl_of = {(x.upper() + "/USD"): x for x in assets}             # BTC/USD  -> btc
    scanned = {"binance": 0, "rtds": 0}
    for day in a.days_list:
        for _d, name in K.files_of(a, "binance", [day]):
            for line in K.iter_lines(a.bucket, a.prefix, "binance", day, name):
                scanned["binance"] += 1
                t, raw = K.parse_rec(line)
                if not isinstance(raw, dict) or "@" not in str(raw.get("stream", "")):
                    continue
                st = str(raw["stream"]).lower()
                base, _, kind = st.partition("@")
                asset = base_of.get(base)
                if not asset or "aggtrade" not in kind:
                    continue
                d = raw.get("data") or {}
                try:
                    px = float(d["p"])
                    ts = float(d.get("T") or d.get("E") or 0) / 1000.0 or (t or 0) / 1e9
                    vol = float(d.get("q") or 0)
                except Exception:
                    continue
                if ts > 1e9:
                    tb[asset].append((ts, px, vol))
        for _d, name in K.files_of(a, "rtds", [day]):
            for line in K.iter_lines(a.bucket, a.prefix, "rtds", day, name):
                scanned["rtds"] += 1
                t, raw = K.parse_rec(line)
                if not isinstance(raw, dict):
                    continue
                pay = raw.get("payload") if isinstance(raw.get("payload"), dict) else raw
                if not isinstance(pay, dict):
                    continue
                asset = cl_of.get(str(pay.get("symbol", "")).upper())
                if not asset:
                    continue
                recv = (t or 0) / 1e9
                pts = pay.get("data")
                batch_ts = float(pay.get("timestamp") or 0)
                batch_ts = batch_ts / 1000.0 if batch_ts > 1e12 else batch_ts
                if isinstance(pts, list) and pts and isinstance(pts[0], dict):
                    for pt in pts:
                        try:
                            v = float(pt.get("value", pt.get("price")))
                            ts = float(pt.get("timestamp", batch_ts))
                            ts = ts / 1000.0 if ts > 1e12 else ts
                            clp[asset].append((ts, v, recv))
                        except Exception:
                            pass
                elif pay.get("value") is not None:
                    try:
                        clp[asset].append((batch_ts, float(pay["value"]), recv))
                    except Exception:
                        pass
        print(f"  [{day}] просканировано строк: binance {scanned['binance']:,}, rtds {scanned['rtds']:,}", flush=True)
    for x in assets:
        tb[x].sort(key=lambda r: r[0])
        clp[x].sort(key=lambda r: r[0])
    return tb, clp


def minute_bars(trades):
    """1-мин бары из тиков: {minute_start: [o,h,l,c,vol,qv,n]}."""
    bars = {}
    for ts, px, vol in trades:
        m = int(ts // 60) * 60
        b = bars.get(m)
        if b is None:
            bars[m] = [px, px, px, px, vol, px * vol, 1]
        else:
            if px > b[1]:
                b[1] = px
            if px < b[2]:
                b[2] = px
            b[3] = px
            b[4] += vol
            b[5] += px * vol
            b[6] += 1
    return bars


def rollup(bars, tf):
    """1-мин бары -> tf-секундные бары: [(t0, o,h,l,c,vol,close_vwap, n_trades)]."""
    out = []
    cur = None
    for m in sorted(bars):
        b = bars[m]
        t0 = m // tf * tf
        if cur and cur[0] == t0:
            cur[2] = max(cur[2], b[1])
            cur[3] = min(cur[3], b[2])
            cur[4] = b[3]
            cur[5] += b[4]
            cur[6] += b[5]
            cur[7] += b[6]
        else:
            if cur:
                out.append(tuple(cur))
            vwap = b[5] / b[4] if b[4] > 0 else b[3]
            cur = [t0, b[0], b[1], b[2], b[3], b[4], vwap, b[6]]
    if cur:
        out.append(tuple(cur))
    return out


def std(xs):
    n = len(xs)
    if n < 2:
        return 0.0
    mu = sum(xs) / n
    return math.sqrt(sum((x - mu) ** 2 for x in xs) / (n - 1))


def build_table(a, asset, trades, cl):
    fb = 0
    if len(trades) < 600:
        print(f"  [{asset}] мало тиков ({len(trades)}) — пропуск")
        return None
    m1 = minute_bars(trades)
    bs = rollup(m1, a.tf)
    kmap = {m: (b[0], b[1], b[2], b[3], b[4], b[5], b[6]) for m, b in m1.items()}
    kmin = sorted(kmap)
    import bisect
    cl_t = [t for t, _, _ in cl]
    rows = []
    hs = [(h, max(1, int(h * 60 / a.tf))) for h in a.horizons_min]
    for i in range(len(bs)):
        t0, o, h, l, c, vol, vwap, n = bs[i]
        if i < 12:
            continue
        close_prev = bs[i - 1][4]
        r = lambda j: 1e4 * (bs[i][4] / bs[i - j][4] - 1.0) if bs[i - j][4] > 0 else 0.0
        rets12 = [1e4 * math.log(bs[i - j + 1][4] / bs[i - j][4]) for j in range(1, 13) if bs[i - j][4] > 0]
        # CL-снимок на момент закрытия бара
        ci = bisect.bisect_right(cl_t, t0 + a.tf) - 1
        if ci >= 0:
            cl_off = 1e4 * (cl[ci][1] / c - 1.0)
            cl_age = (t0 + a.tf) - cl[ci][0]
        else:
            cl_off, cl_age = 0.0, -1.0
        hour = ((t0 + a.tf) % 86400) / 86400.0 * 2 * math.pi
        row = {
            "t0": t0,
            "r1": round(r(1), 3), "r3": round(r(3), 3), "r12": round(r(12), 3),
            "volat12": round(std(rets12), 3) if len(rets12) > 3 else 0.0,
            "range_bps": round(1e4 * (h - l) / c, 3) if c else 0.0,
            "vwap_dev_bps": round(1e4 * (c / vwap - 1.0), 3) if vwap else 0.0,
            "cl_off_bps": round(cl_off, 3), "cl_age_s": round(cl_age, 1),
            "n_trades": int(math.log1p(n)),
            "hour_sin": round(math.sin(hour), 4), "hour_cos": round(math.cos(hour), 4),
            "close": c,
        }
        ok_labels = True
        for hm, kk in hs:
            j = i + kk
            if j >= len(bs):
                ok_labels = False
                break
            mv = 1e4 * (bs[j][4] / c - 1.0) if c else 0.0
            cls = 0 if abs(mv) <= a.floor_bps else (1 if mv > 0 else -1)
            row[f"move_bps_{hm}m"] = round(mv, 3)
            row[f"dir_{hm}m"] = (1 if mv > 0 else -1)
            row[f"cls_{hm}m"] = cls
        if ok_labels:
            rows.append(row)
    print(f"  [{asset}] тиков {len(trades)} (ts из приёма {fb}), 1м-баров {len(m1)}, "
          f"{a.tf}-баров {len(bs)}, строк с метками {len(rows)}; CL-точек {len(cl)}")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="kronolog")
    ap.add_argument("--days", required=True, help="20260903-20260909 или список через запятую")
    ap.add_argument("--tf", type=int, default=300, help="горизонт бара, сек (300=5м)")
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    ap.add_argument("--horizons", default=str(ap.get_default("tf") // 60),
                    help="метки через N минут, через запятую (например 5,10,15)")
    ap.add_argument("--floor-bps", type=float, default=4.7)
    ap.add_argument("--files", type=int, default=0, help="0 = все файлы дня (рекомендуется)")
    ap.add_argument("--outdir", default="/tmp/ds")
    ap.add_argument("--cache", default="", help="папка локального кэша; '' = без кэша")
    a = ap.parse_args()
    a.days_list = K.days_of(argparse.Namespace(days=a.days, day=None, mode="x")) if hasattr(K, "days_of") else []
    if not a.days_list:
        x, y = a.days.split("-", 1) if "-" in a.days else (a.days.split(",")[0],) * 2
        import datetime as dt
        d0 = dt.datetime.strptime(x, "%Y%m%d"); d1 = dt.datetime.strptime(y, "%Y%m%d")
        a.days_list = [(d0 + dt.timedelta(i)).strftime("%Y%m%d") for i in range((d1 - d0).days + 1)]
    a.horizons_min = [int(float(x)) for x in str(a.horizons).split(",")]
    if a.cache:
        K.CACHE = a.cache
        import subprocess
        for stream in ("binance", "rtds"):
            for day in a.days_list:
                dst = os.path.join(a.cache, a.bucket, a.prefix, stream, day)
                done = os.path.join(dst, ".done")
                if os.path.exists(done):
                    print(f"  кэш {stream}/{day}: уже скачан, пропуск")
                    continue
                os.makedirs(dst, exist_ok=True)
                print(f"  sync {stream}/{day} -> {dst} ...", flush=True)
                r = subprocess.run(
                    f"aws s3 sync s3://{a.bucket}/{a.prefix}/{stream}/{day}/ {dst}/",
                    shell=True)
                if r.returncode != 0:
                    print(f"  WARNING: sync {stream}/{day} вернул {r.returncode} — "
                          f"этот день будет читаться из S3 напрямую")
                else:
                    open(done, "w").write("ok")
    os.makedirs(a.outdir, exist_ok=True)
    print(f"# dataset v{VERSION}: дни {a.days_list[0]}..{a.days_list[-1]}  tf={a.tf}s  "
          f"floor={a.floor_bps} б.п.  метки {a.horizons_min}м")
    meta = {"version": VERSION, "generated_utc": __import__("time").strftime("%Y-%m-%dT%H:%MZ"),
            "days": a.days_list, "tf_s": a.tf, "floor_bps": a.floor_bps,
            "horizons_min": a.horizons_min, "assets": {},
            "columns": {"t0": "время ЗАКРЫТИЯ бара (unix s) — фичи по данные <= t0",
                        "r1/r3/r12": "доходность за 1/3/12 баров, б.п.",
                        "volat12": "стdev 1-баровых доходностей (×12 баров), б.п.",
                        "range_bps": "размах бара, б.п.", "vwap_dev_bps": "close/vwap внутри бара, б.п.",
                        "cl_off_bps": "последняя цена Chainlink vs close бара, б.п.",
                        "cl_age_s": "возраст CL-цены на закрытии бара, сек (-1 = нет данных)",
                        "n_trades": "log1p(число тиков бара)",
                        "hour_sin/hour_cos": "время суток", "close": "цена закрытия бара",
                        "move_bps_*m": "будущее движение (метка!)",
                        "dir_*m": "1/-1 (метка!)", "cls_*m": "1/0/-1 с порогом flat (метка!)"}}
    assets = [s.strip().lower() for s in a.assets.split(",") if s.strip()]
    trades_by, cl_by = collect(a, assets)
    for asset in assets:
        rows = build_table(a, asset, trades_by[asset], cl_by[asset])
        if not rows:
            continue
        cols = list(rows[0].keys())
        path = os.path.join(a.outdir, f"{asset}_{a.tf}s.csv")
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        meta["assets"][asset] = {"file": path, "rows": len(rows), "columns": cols}
        try:
            import numpy as np
            arr = np.array([[r[c] for c in cols] for r in rows], dtype="float64")
            np.savez_compressed(os.path.join(a.outdir, f"{asset}_{a.tf}s.npz"),
                                 X=arr, cols=np.array(cols))
        except Exception:
            pass
        # баланс классов по первому горизонту
        hm = a.horizons_min[0]
        cc = defaultdict(int)
        for r in rows:
            cc[r[f"cls_{hm}m"]] += 1
        n = len(rows) or 1
        print(f"  [{asset}] классы {hm}м: up {cc[1]/n:.1%} / flat {cc[0]/n:.1%} / down {cc[-1]/n:.1%}")
    with open(os.path.join(a.outdir, "meta.json"), "w") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)
    print(f"готово: {a.outdir}/  (+ meta.json)")
    try:
        import numpy  # noqa: F401
    except Exception:
        print("подсказка: npz пропущен — numpy нет, csv полностью рабочий")


if __name__ == "__main__":
    main()
