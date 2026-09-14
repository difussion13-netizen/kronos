#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""clobwin.py — датасет «окно → книга» из clob-лога (+ минутки из binance-лога).

Стриминг без скачивания на диск: `aws s3 cp - | zcat` построчно, разбор регулярками
(clob ~8.8ГБ/сутки — JSON-парсер на каждую строку не потянем). Формат записи логгера:
{"t":<ns>, "raw":{...WS-сообщение CLOB...}}, события book / price_change / last_trade.

Порядок на сутки: A) regex-проход -> множество asset_id; B) Gamma API -> карта
token->окно (slug <asset>-updown-<code>-<start_epoch>); C) стрим книг: топ bid/ask
замораживается на чекпоинтах «за c секунд до конца окна» + счётчики потока.

    python3 clobwin.py --bucket B --days 20260912-20260912 --outdir /tmp/win --minutes
Карта токенов кэшируется (outdir/tokens_map.json); повторные прогоны дёшевы.
"""
import argparse
import csv
import datetime as dt
import json
import os
import re
import subprocess
import sys
import urllib.request

RX_ID = re.compile(rb'"asset_id":"(\d+)"')
RX_TS = re.compile(rb'"timestamp":"?(\d{10,13})"?')
RX_ENV_T = re.compile(rb'^\{"t":(\d{13,20})')
RX_EVENT = re.compile(rb'"event_type":"(\w+)"')
RX_BK = re.compile(rb'"bids":\[([^\]]*)\]')
RX_AK = re.compile(rb'"asks":\[([^\]]*)\]')
RX_CHGSEG = re.compile(rb'"changes":\[([^\]]*)\]')
RX_PPS = re.compile(rb'"price":\s*"?([\d.]+)"?[^{}]*?"size":\s*"?([\d.]+)')
RX_SIDE = re.compile(rb'"side":"(\w+)"')
RX_LT = re.compile(rb'"price":\s*"?([\d.]+)"?[^}]*?"size":\s*"?([\d.]+)')
RX_AGGT = re.compile(rb'"(btcusdt|ethusdt|solusdt|xrpusdt)@aggTrade"')

CPS = {"5m": (240, 120, 60, 30, 15, 5, 0),
       "15m": (600, 240, 120, 60, 30, 15, 5, 0),
       "4h": (1800, 600, 240, 120, 60, 30, 15, 5, 0)}
CODE_S = {"5m": 300, "15m": 900, "4h": 14400}
SLUG_RE = re.compile(r"^([a-z]+)-updown-(\w+)-(\d+)$")


def sh_pipe(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL)


def day_files(bucket, prefix, stream, day):
    r = subprocess.run(f"aws s3 ls s3://{bucket}/{prefix}/{stream}/{day}/",
                       shell=True, capture_output=True, text=True)
    out = []
    for ln in r.stdout.splitlines():
        p = ln.split()
        if len(p) >= 4 and p[3].endswith(".jsonl.gz"):
            out.append(p[3])
    return sorted(out)


def iter_lines(bucket, prefix, stream, day, name):
    p = sh_pipe(f"aws s3 cp s3://{bucket}/{prefix}/{stream}/{day}/{name} - | zcat")
    for ln in p.stdout:
        yield ln
    p.wait()


HDRS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) kronos-research/1.0",
        "Accept": "application/json"}


def gamma_map(ids, gamma):
    m = {}
    ids = sorted(ids)
    fails = 0
    for i in range(0, len(ids), 40):
        q = ",".join(ids[i:i + 40])
        try:
            req = urllib.request.Request(f"{gamma}/markets?clob_token_ids={q}", headers=HDRS)
            with urllib.request.urlopen(req, timeout=20) as r:
                arr = json.loads(r.read().decode())
        except Exception as e:
            fails += 1
            if fails == 1:
                print(f"  gamma batch {i}: {e}", file=sys.stderr)
            continue
        for mk in arr:
            slug = mk.get("slug") or ""
            s = SLUG_RE.match(slug)
            if not s or s.group(2) not in CODE_S:
                continue
            toks = mk.get("clobTokenIds")
            outs = mk.get("outcomes")
            try:
                toks = json.loads(toks) if isinstance(toks, str) else (toks or [])
                outs = json.loads(outs) if isinstance(outs, str) else (outs or [])
                up = outs.index("Up")
            except Exception:
                up = 0
            for k, t in enumerate(toks):
                m[str(t)] = {"slug": slug, "asset": s.group(1), "code": s.group(2),
                             "start": int(s.group(3)), "up": (k == up)}
        print(f"  gamma: размечено {min(i + 40, len(ids))}/{len(ids)} токенов", flush=True)
    if fails and not m:
        raise SystemExit(
            "Gamma недоступна с этого IP/UA. План Б — запускать clobwin на сервере\n"
            "логгера (там Gamma работает):  scp скрипт, либо git clone + python3\n"
            "clobwin.py --bucket ... --day ...; детали в docs/plan-4-evals.md.")
    return m


def collect_ids(a, day, files):
    seen, ev, n = set(), {}, 0
    nf = len(files)
    for fi, name in enumerate(files):
        for ln in iter_lines(a.bucket, a.prefix, "clob", day, name):
            n += 1
            mid = RX_ID.search(ln)
            if mid:
                seen.add(mid.group(1).decode())
            mev = RX_EVENT.search(ln)
            if mev:
                ev[mev.group(1).decode()] = ev.get(mev.group(1).decode(), 0) + 1
        if fi % 20 == 19 or fi + 1 == nf:
            print(f"  [{day}/passA] файл {fi+1}/{nf}, строк {n:,}, токенов {len(seen)}", flush=True)
    print(f"  [{day}/passA] итог: строк {n:,}; токенов {len(seen)}; события {ev}", flush=True)
    return seen


def msg_ts(ln):
    m = RX_TS.search(ln)
    if m:
        v = int(m.group(1))
        return v / 1000.0 if v > 10 ** 12 else float(v)
    m = RX_ENV_T.match(ln)
    return int(m.group(1)) / 1e9 if m else 0.0


def run_day(a, day, tokmap):
    files = day_files(a.bucket, a.prefix, "clob", day)
    wins = {}
    n_lines = matched = 0
    nf = len(files)
    for fi, name in enumerate(files):
        if fi % 20 == 19 or fi + 1 == nf:
            print(f"  [{day}/passC] файл {fi+1}/{nf}, строк {n_lines:,}, окон {len(wins)}", flush=True)
        for ln in iter_lines(a.bucket, a.prefix, "clob", day, name):
            n_lines += 1
            mid = RX_ID.search(ln)
            if not mid:
                continue
            info = tokmap.get(mid.group(1).decode())
            if not info:
                continue
            matched += 1
            key = (info["start"], info["asset"], info["code"])
            w = wins.get(key)
            if w is None:
                cps = list(CPS.get(info["code"]) or CPS["5m"])
                w = wins[key] = {"slot": {}, "pend": cps, "prev": (None, None),
                                 "bids": {}, "asks": {},
                                 "n_book": 0, "n_pc": 0, "n_lt": 0, "vol": 0.0,
                                 "end": info["start"] + CODE_S.get(info["code"], 300)}
            ev = RX_EVENT.search(ln)
            ev = ev.group(1) if ev else b"?"
            if ev.startswith(b"last_trade"):              # ..._price у реальных сообщений; оба токена — тот же рынок
                w["n_lt"] += 1
                ml = RX_LT.search(ln)
                if ml:
                    try:
                        w["vol"] += float(ml.group(1)) * float(ml.group(2))
                    except ValueError:
                        pass
                continue
            if not info["up"]:
                continue                                  # состояние книги — по Up-токену
            if ev == b"book":
                w["n_book"] += 1
                w["bids"], w["asks"] = {}, {}
                for seg, into in ((RX_BK.search(ln), w["bids"]), (RX_AK.search(ln), w["asks"])):
                    if not seg:
                        continue
                    for obj in re.findall(rb"\{[^{}]*\}", seg.group(1)):
                        m = RX_PPS.search(obj)
                        if m:
                            try:
                                into[float(m.group(1))] = float(m.group(2))
                            except ValueError:
                                pass
            elif ev == b"price_change":
                w["n_pc"] += 1
                seg = RX_CHGSEG.search(ln)
                if seg:
                    for obj in re.findall(rb"\{[^{}]*\}", seg.group(1)):
                        m = RX_PPS.search(obj)
                        sd = RX_SIDE.search(obj)
                        if not m or not sd:
                            continue
                        try:
                            p = float(m.group(1)); sz = float(m.group(2))
                        except ValueError:
                            continue
                        into = w["bids"] if sd.group(1) == b"BUY" else w["asks"]
                        other = w["asks"] if sd.group(1) == b"BUY" else w["bids"]
                        if sz > 0:
                            into[p] = sz
                            # пересёк встречную сторону = исполнится/снимется (кроссинг)
                            for q in [o for o in other if (o <= p if sd.group(1) == b"BUY" else o >= p)]:
                                del other[q]
                        else:
                            into.pop(p, None)
            rem = w["end"] - msg_ts(ln)
            while w["pend"] and rem < w["pend"][0]:
                c = w["pend"].pop(0)
                w["slot"][c] = w["prev"]
            if w["bids"] and w["asks"]:
                w["prev"] = (round(max(w["bids"]), 4), round(min(w["asks"]), 4))
    for w in wins.values():                               # остаток хвостов: текущее состояние
        while w["pend"]:
            c = w["pend"].pop(0)
            w["slot"][c] = w["prev"]
    print(f"  [{day}/passC] строк {n_lines:,}; размечено {matched:,}; окон {len(wins)}", flush=True)
    out = os.path.join(a.outdir, f"windows_{day}.csv")
    cp_all = sorted(set(CPS["5m"]) | set(CPS["15m"]) | set(CPS["4h"]), reverse=True)
    with open(out, "w", newline="") as f:
        wc = csv.writer(f)
        head = ["start", "end", "asset", "code", "n_book", "n_pc", "n_lt", "vol_usd"]
        for c in cp_all:
            head += [f"b{c}", f"a{c}"]
        wc.writerow(head)
        for (start, asset, code), w in sorted(wins.items()):
            row = [start, w["end"], asset, code, w["n_book"], w["n_pc"], w["n_lt"], round(w["vol"], 1)]
            for c in cp_all:
                v = w["slot"].get(c, (None, None))
                row += ["" if x is None else x for x in v]
            wc.writerow(row)
    print(f"  [{day}] -> {out}", flush=True)


def do_minutes(a, day, assets):
    rows = {b: {} for b in assets}
    n = 0
    bf = day_files(a.bucket, a.prefix, "binance", day)
    nb = len(bf)
    for bi, name in enumerate(bf):
        if bi % 40 == 39:
            print(f"  [{day}/minutes] файл {bi+1}/{nb}, строк {n:,}", flush=True)
        for ln in iter_lines(a.bucket, a.prefix, "binance", day, name):
            if b"@aggTrade" not in ln:
                continue
            n += 1
            ms = RX_AGGT.search(ln)
            if not ms:
                continue
            base = ms.group(1).decode()[:3]
            mt = re.search(rb'"T":\s*(\d{10,16})', ln)
            mp = re.search(rb'"p":\s*"?([\d.]+)', ln)
            if not (mt and mp):
                continue
            ts = int(mt.group(1)) / 1000.0
            rows[base][int(ts // 60) * 60] = float(mp.group(1))
    out = os.path.join(a.outdir, f"minutes_{day}.csv")
    with open(out, "w", newline="") as f:
        wc = csv.writer(f)
        wc.writerow(["day", "minute", "asset", "close"])
        for base, d in sorted(rows.items()):
            for m, px in sorted(d.items()):
                wc.writerow([day, m, base, px])
    print(f"  [{day}/minutes] aggTrade {n:,} -> {out}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="kronolog")
    ap.add_argument("--day")
    ap.add_argument("--days", help="A-B или список")
    ap.add_argument("--outdir", default="/tmp/win")
    ap.add_argument("--gamma", default="https://gamma-api.polymarket.com")
    ap.add_argument("--minutes", action="store_true")
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    a = ap.parse_args()
    days = []
    if a.day:
        days = [a.day]
    elif a.days:
        if "-" in a.days:
            x, y = a.days.split("-")
            d0 = dt.datetime.strptime(x, "%Y%m%d"); d1 = dt.datetime.strptime(y, "%Y%m%d")
            while d0 <= d1:
                days.append(d0.strftime("%Y%m%d")); d0 += dt.timedelta(days=1)
        else:
            days = a.days.split(",")
    os.makedirs(a.outdir, exist_ok=True)
    map_path = os.path.join(a.outdir, "tokens_map.json")
    tokmap = json.load(open(map_path)) if os.path.exists(map_path) else {}
    if tokmap:
        print(f"карта токенов: {len(tokmap)} из кэша")
    for day in days:
        files = day_files(a.bucket, a.prefix, "clob", day)
        if not files:
            print(f"  [{day}] нет clob-файлов — пропуск")
            continue
        ids = collect_ids(a, day, files)
        new_ids = [x for x in ids if x not in tokmap]
        if new_ids:                       # карта пополняется приростом (окна меняются ежедневно)
            tokmap.update(gamma_map(new_ids, a.gamma))
            json.dump(tokmap, open(map_path, "w"))
            print(f"  карта: +{len(new_ids)} новых, всего {len(tokmap)}")
        run_day(a, day, tokmap)
        if a.minutes:
            do_minutes(a, day, set(x.strip().lower() for x in a.assets.split(",")))
    print("готово:", a.outdir)


if __name__ == "__main__":
    main()
