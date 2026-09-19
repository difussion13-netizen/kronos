#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""clobq.py — статистика очереди по сырым clob-логам (M6, артефакт «fill-данные»).

Идея: окно up/down уже размечено картой (tokens_map.json). Для каждого токена
прокручиваем его поток событий (book / price_change / last_trade) и на каждом
чекпоинте (t−cp секунд до конца окна, те же cp что у clobwin) снимаем top-5
уровней bid/ask с размерами. До конца окна ведём по каждому снятому уровню:
минимум размера после чекпоинта, размер на момент конца, суммарный объём
сделок, прошедших ПО ЭТОМУ уровню (тейк-сторона, которая его ест).
Строка csv = (окно, cp, сторона, ранг, цена, S_at, S_end, S_min, traded_at,
consumed_frac). Из таблицы напрямую оценивается
P(уровень съеден до конца | S, Δt) — а наша P(fill), стоя за объёмом S,
аппроксимируется событием «traded_at ≥ S + size_нашей_заявки» (FIFO-консерватив:
встаём ЗА весь видимый объём; реальный порядок очереди — в clobcap поправим).

Поток — как у clobwin: `aws s3 cp - | zcat` построчно, разбор регулярками.
Карту читает по files, НИЧЕГО в S3 не пишет (read-only — канон не пачкается).

  python3 clobq.py --bucket B --days 20260910-20260914 --outdir ~/q1
Локальный прогон (тесты): --local-dir DIR с *.jsonl.gz вместо S3.
"""
import argparse
import gzip
import json
import os
import re
import subprocess
import sys
import time

RX_ID = re.compile(rb'"asset_id":"(\d+)"')
RX_EV = re.compile(rb'"event_type":"(\w+)"')
RX_TS = re.compile(rb'"timestamp":"?(\d{10,13})"?')
RX_BK = re.compile(rb'"(bids|asks)":\[([^\]]*)\]')
RX_NUM = rb'"%s":\s*"?([\d.]+)"?'   # кавычки необязательны — как в clobwin
RX_CHANGES = re.compile(rb'"changes":\[.*?\]\s*[,}]', re.S)
RX_PSO = re.compile(rb'"price":"([\d.]+)"[^{}]*?"side":"(\w+)"[^{}]*?"size":"([\d.]+)"')
RX_PS = re.compile(rb'"price":\s*"?([\d.]+)"?[^{}]*?"size":\s*"?([\d.]+)"?')
RX_SIDE = re.compile(rb'"side":"(\w+)"')
RX_SLUG = re.compile(r"^([a-z]+)-updown-(\w+)-(\d+)$")

CPS = {"5m": (240, 120, 60, 30, 15, 5, 0),
       "15m": (600, 240, 120, 60, 30, 15, 5, 0),
       "4h": (1800, 600, 240, 120, 60, 30, 15, 5, 0)}


def sh_pipe(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL)


def day_files(bucket, prefix, day, local_dir=None):
    if local_dir:
        d = os.path.join(local_dir, day) if os.path.isdir(
            os.path.join(local_dir, day)) else local_dir
        return [os.path.join(d, f) for f in sorted(os.listdir(d))
                if f.endswith(".jsonl.gz")]
    r = subprocess.run(f"aws s3 ls s3://{bucket}/{prefix}/clob/{day}/",
                       shell=True, capture_output=True, text=True)
    out = []
    for ln in r.stdout.splitlines():
        p = ln.split()
        if len(p) >= 4 and p[3].endswith(".jsonl.gz"):
            out.append(f"s3://{bucket}/{prefix}/clob/{day}/{p[3]}")
    return sorted(out)


def open_lines(bucket, f, local_dir):
    if local_dir:
        return gzip.open(f, "rt", encoding="utf-8", errors="replace")
    return sh_pipe(f"aws s3 cp {f} - | zcat").stdout


def norm_ts(raw, t_ns):
    """Событийный timestamp в секундах; fallback — envelope t (ns/мс/с)."""
    try:
        v = int(raw)
    except (TypeError, ValueError):
        v = None
    if v:
        return v / 1000.0 if v > 10 ** 11 else float(v)
    v = int(t_ns or 0)
    return v / 1e9 if v > 10 ** 14 else v / 1e3


class TokenState:
    __slots__ = ("bids", "asks", "cps", "last_ts")

    def __init__(self):
        self.bids = {}      # price(float)->size(float)
        self.asks = {}
        self.cps = {}       # cp_offset -> {"snap": {side:{price:(size,min,end,tra)}}, "done": bool}
        self.last_ts = 0.0


def apply_book(st, seg, side):
    d = st.bids if side == "bids" else st.asks
    pairs = RX_PS.findall(seg)
    if not pairs:
        d.clear()
        return
    d.clear()
    for p, s in pairs:
        try:
            d[float(p)] = float(s)
        except ValueError:
            pass


def apply_change(st, price, side, size):
    d = st.bids if side.upper() == "BUY" else st.asks
    if size <= 0:
        d.pop(price, None)
    else:
        d[price] = size


class QueuePass:
    def __init__(self, tokmap, day):
        # token -> (start,end,asset,code,cps)
        self.win = {}
        for tok, info in tokmap.items():
            m = RX_SLUG.match(info["slug"])
            if not m:
                continue
            asset, code, start = m.group(1), m.group(2), int(m.group(3))
            dur = {"5m": 300, "15m": 900, "4h": 14400}[code]
            end = start + dur
            # окно должно относиться к суткам (start в дне или end в дне)
            if not (day[0] <= start < day[1] or day[0] < end <= day[1]):
                continue
            self.win[tok] = (start, end, asset, code, CPS[code])
        self.states = {}
        self.rows = []
        self.n_tok_events = 0

    def touch_cps(self, tok, ts):
        w = self.win.get(tok)
        if w is None:
            return
        start, end, _a, _c, cps = w
        st = self.states.setdefault(tok, TokenState())
        for cp in cps:
            at = end - cp
            if cp not in st.cps and st.last_ts < at <= ts:
                st.cps[cp] = {
                    "snap_b": [(p, sz, sz, sz, 0.0) for p, sz in
                               sorted(st.bids.items(), key=lambda x: -x[0])[:5]],
                    "snap_a": [(p, sz, sz, sz, 0.0) for p, sz in
                               sorted(st.asks.items(), key=lambda x: x[0])[:5]],
                }
        st.last_ts = ts

    def on_event(self, tok, ts, ev, raw):
        st = self.states.get(tok)
        if st is None:
            if tok not in self.win:
                return
            st = self.states[tok] = TokenState()
        self.n_tok_events += 1
        self.touch_cps(tok, ts)
        if ev == b"book":
            for m in RX_BK.finditer(raw):
                apply_book(st, m.group(2), m.group(1).decode())
        elif ev == b"price_change":
            seg = RX_CHANGES.search(raw)
            if seg:
                # поля могут идти в любом порядке — достаем по объекту
                for o in re.finditer(rb'\{[^{}]*\}', seg.group(0)):
                    ob = o.group(0)
                    mp = re.search(rb'"price":\s*"?([\d.]+)"?', ob)
                    ms = re.search(rb'"side":"(\w+)"', ob)
                    mz = re.search(rb'"size":\s*"?([\d.]+)"?', ob)
                    if not (mp and ms and mz):
                        continue
                    try:
                        apply_change(st, float(mp.group(1)), ms.group(1).decode(),
                                     float(mz.group(1)))
                    except ValueError:
                        pass
        elif ev.startswith(b"last_trade"):   # реальное имя: last_trade_price
            mp = re.search(rb'"price":\s*"?([\d.]+)"?', raw)
            mz = re.search(rb'"size":\s*"?([\d.]+)"?', raw)
            ms = RX_SIDE.search(raw)
            if ms and mp and mz:
                tr_side = ms.group(1).decode().upper()  # BUY -> ест asks
                price = float(mp.group(1))
                size = float(mz.group(1))
                key = "snap_a" if tr_side == "BUY" else "snap_b"
                for cp, c in st.cps.items():
                    snap = c[key]
                    for i, (pp, s0, mn, en, tr) in enumerate(snap):
                        if pp == price:
                            snap[i] = (pp, s0, mn, en, tr + size)
        # после обновления — ведём running min/end по снятым уровням
        for cp, c in st.cps.items():
            for key, d in (("snap_b", st.bids), ("snap_a", st.asks)):
                snap = c[key]
                for i, (p, s0, mn, en, tr) in enumerate(snap):
                    cur = d.get(p, 0.0)
                    mn = min(mn, cur)
                    snap[i] = (p, s0, mn, cur, tr)

    def flush(self):
        for tok, st in self.states.items():
            w = self.win.get(tok)
            if not w:
                continue
            start, end, asset, code, _ = w
            for cp, c in sorted(st.cps.items()):
                for side_key, side in (("snap_b", "bid"), ("snap_a", "ask")):
                    for rank, (p, s0, mn, en, tr) in enumerate(c[side_key]):
                        if s0 <= 0:
                            continue
                        self.rows.append(dict(
                            day=None, tok=tok, start=start, asset=asset, code=code,
                            cp=cp, side=side, rank=rank, price=p, s_at=s0,
                            s_end=en, s_min=mn, traded=tr,
                            consumed_frac=round((s0 - min(mn, en)) / s0, 4)))


def load_map(a, outdir):
    p = os.path.join(outdir, "tokens_map.json")
    if a.map_local:
        p = a.map_local
    if not os.path.exists(p) and a.bucket:
        subprocess.run(f"aws s3 cp s3://{a.bucket}/{a.map_s3_key} {p}", shell=True)
    return json.load(open(p))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default="")
    ap.add_argument("--prefix", default="kronolog")
    ap.add_argument("--days", required=True, help="A-B или список")
    ap.add_argument("--outdir", default="~/q1")
    ap.add_argument("--map-local", default="")
    ap.add_argument("--map-s3-key", default="kronos/win/tokens_map.json")
    ap.add_argument("--local-dir", default=None, help="тесты: папка с .jsonl.gz")
    ap.add_argument("--max-files", type=int, default=0)
    a = ap.parse_args()
    outdir = os.path.expanduser(a.outdir)
    os.makedirs(outdir, exist_ok=True)
    tokmap = load_map(a, outdir)
    print(f"карта: {len(tokmap)} токенов", flush=True)

    if "-" in a.days:
        import datetime as dt
        x, y = a.days.split("-")
        d0 = dt.datetime.strptime(x, "%Y%m%d").date()
        d1 = dt.datetime.strptime(y, "%Y%m%d").date()
        days = [(d0 + dt.timedelta(days=i)).strftime("%Y%m%d")
                for i in range((d1 - d0).days + 1)]
    else:
        days = a.days.split(",")

    t_all = time.time()
    for day in days:
        import datetime as dtm
        d0 = dtm.datetime.strptime(day, "%Y%m%d").replace(tzinfo=dtm.timezone.utc)
        day_span = (d0.timestamp(), d0.timestamp() + 86400)
        qp = QueuePass(tokmap, day_span)
        files = day_files(a.bucket, a.prefix, day, a.local_dir)
        if a.max_files:
            files = files[:a.max_files]
        for fi, f in enumerate(files):
            with open_lines(a.bucket, f, a.local_dir) as fh:
                for ln in fh:
                    if isinstance(ln, bytes):
                        b = ln
                    else:
                        b = ln.encode()
                    mi = RX_ID.search(b)
                    if not mi:
                        continue
                    ev = RX_EV.search(b)
                    if not ev:
                        continue
                    mts = RX_TS.search(b)
                    ts = norm_ts(mts.group(1) if mts else None, None)
                    if not ts:
                        mt = re.match(rb'^\{"t":(\d{13,20})', b)
                        if mt:
                            ts = norm_ts(None, mt.group(1))
                    qp.on_event(mi.group(1).decode(), ts, ev.group(1), b)
            if fi % 20 == 0 or fi == len(files) - 1:
                print(f"  [{day}] файл {fi+1}/{len(files)}, событий по окнам "
                      f"{qp.n_tok_events:,}", flush=True)
        qp.flush()
        out = os.path.join(outdir, f"queue_{day}.csv")
        import csv
        cols = ["day", "tok", "start", "asset", "code", "cp", "side", "rank",
                "price", "s_at", "s_end", "s_min", "traded", "consumed_frac"]
        with open(out, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for r in qp.rows:
                r["day"] = day
                w.writerow(r)
        # сводка на лету
        by = {}
        for r in qp.rows or [{}]:
            if "code" not in r:
                continue
            k = (r["code"], r["cp"])
            e = by.setdefault(k, [0, 0, 0.0])
            e[0] += 1
            if r["s_min"] <= 0 or r["traded"] >= r["s_at"]:
                e[1] += 1
            e[2] += r["consumed_frac"]
        print(f"  [{day}] строк {len(qp.rows):,} → {out}; "
              f"P(уровень съеден | code,cp) и avg consumed:", flush=True)
        for k in sorted(by, key=lambda x: (x[0], -x[1])):
            n, eated, cs = by[k]
            print(f"    {k[0]:>3} cp={k[1]:<5} n={n:<8} P(eat)={eated/n:.3f} "
                  f"cons={cs/n:.3f}", flush=True)
    print(f"итого {time.time()-t_all:.0f} с", flush=True)


if __name__ == "__main__":
    main()
