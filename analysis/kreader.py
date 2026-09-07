#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""kreader.py — читалка сырых логов kronolog из S3.

Сырые файлы: s3://BUCKET/kronolog/<stream>/<YYYYMMDD>/<stream>_<YYYYMMDD>_<HHMMSS>.jsonl.gz
Строка внутри:  {"t": <время_приёма_нс>, "raw": <исходный JSON>}  (или "text": строка).

Режимы (подкоманды):
    probe   — показать, что РЕАЛЬНО лежит внутри raw (скелеты объектов). С него
              стоит начать, если парсер чего-то не находит.
    stats   — дни × потоки: файлов, строк, байт, span времени.
    lag     — Chainlink (rtds) против Binance (binance): возраст цен, ошибка к
              моменту метки, и «время доезда» движения цены до Chainlink.
    candles — пересборка минутных свечей из наших записей (по mid bookTicker).
    verify  — candles против ОФИЦИАЛЬНЫХ klines Binance (публичный REST).
              Совпадение = наш пайплайн читается без потерь.

Зависимости: python3 (3.8+) и `aws` с правами на чтение бакета — т.е. CloudShell.
На сервере EC2 не запускать: роль логгера умеет только писать, читать ей нельзя.

Примеры:
    python3 kreader.py probe  --bucket M --day 20260907 --stream rtds
    python3 kreader.py lag    --bucket M --day 20260907 --sym btc/usd
    python3 kreader.py verify --bucket M --day 20260907 --sym btcusdt
"""
import argparse
import gzip
import io
import json
import re
import statistics
import subprocess
import sys
import urllib.request
from collections import Counter, defaultdict

STREAMS = ("clob", "binance", "rtds", "meta")
_BN_MAP = {"btc/usd": "btcusdt", "eth/usd": "ethusdt", "sol/usd": "solusdt", "xrp/usd": "xrpusdt"}


def to_binance_sym(sym):
    s = sym.lower().replace("/", "")
    if sym.lower() in _BN_MAP:
        return _BN_MAP[sym.lower()]
    if not s.endswith("usdt"):
        s += "usdt"
    return s
def days_of(a):
    """--days A-B | --days A,B | --day A -> список дней."""
    import datetime as _dt
    spec = (a.days or a.day or "")
    if "-" in spec:
        x, y = spec.split("-", 1)
        d0 = _dt.datetime.strptime(x, "%Y%m%d")
        d1 = _dt.datetime.strptime(y, "%Y%m%d")
        return [(d0 + _dt.timedelta(d)).strftime("%Y%m%d") for d in range((d1 - d0).days + 1)]
    return [d for d in spec.split(",") if d]


def files_of(a, stream, days):
    out = []
    for d in days:
        out += [(d, n) for n, _ in day_files(a.bucket, a.prefix, stream, d)]
    if a.files:
        out = out[-int(a.files) * len(days):]
    return out


FKEY_RE = re.compile(r"^(?P<stream>[a-z_]+?)_(?P<day>\d{8})_(?P<time>\d{6})\.jsonl\.gz$")


# ---------------------------------------------------------------- s3 helpers

def sh_bytes(cmd):
    p = subprocess.run(cmd, shell=True, capture_output=True)
    if p.returncode != 0:
        raise SystemExit(f"команда не удалась: {cmd}\n{p.stderr.decode()[:400]}")
    return p.stdout


def day_files(bucket, prefix, stream, day):
    """Файлы потока за день: [(имя, размер_байт)], отсортированные по времени части.
    (aws s3 ls в строке отдаёт «дата время размер ключ» — берём размер и хвост ключа.)"""
    out = sh_bytes(f"aws s3 ls s3://{bucket}/{prefix}/{stream}/{day}/").decode()
    pairs = []
    for line in out.splitlines():
        parts = line.split()
        # формат: дата время размер ключ[/ключ…]; ключ может содержать пробелы — берём хвост
        if len(parts) >= 4 and parts[-1].endswith(".jsonl.gz") and parts[2].isdigit():
            key = " ".join(parts[3:])
            pairs.append((key.rsplit("/", 1)[-1], int(parts[2])))
    return sorted(pairs)


def iter_lines(bucket, prefix, stream, day, name, limit=None):
    """Строки одного .jsonl.gz, потоково (aws s3 cp -> stdout), без temp-файлов."""
    p = subprocess.Popen(f"aws s3 cp s3://{bucket}/{prefix}/{stream}/{day}/{name} -",
                         shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    n = 0
    try:
        with gzip.GzipFile(fileobj=io.BufferedReader(p.stdout, 1 << 20)) as g:
            for raw in io.TextIOWrapper(g, encoding="utf-8", errors="replace"):
                yield raw
                n += 1
                if limit and n >= limit:
                    return
    finally:
        p.stdout.close()
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()


def parse_rec(line):
    try:
        j = json.loads(line)
        return j.get("t"), j.get("raw", j.get("text"))
    except Exception:
        return None, None


# ---------------------------------------------------------------- skeleton (probe)

def skel(obj, depth=0):
    """Компактный «скелет» JSON: поля/типы, списки -> [1 элемент]."""
    if isinstance(obj, dict):
        if depth > 3:
            return "dict{...}"
        return {k: skel(v, depth + 1) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [skel(obj[0], depth + 1)] if obj else []
    if isinstance(obj, bool):
        return "bool"
    if isinstance(obj, (int, float)):
        return "num"
    if isinstance(obj, str):
        return "str"
    return type(obj).__name__


def cmd_probe(a):
    files = [n for n, _ in day_files(a.bucket, a.prefix, a.stream, a.day)]
    if not files:
        raise SystemExit("пусто")
    hist = Counter()
    first = {}
    n = 0
    for f in files[-a.tail:]:
        for line in iter_lines(a.bucket, a.prefix, a.stream, a.day, f):
            t, raw = parse_rec(line)
            if raw is None:
                continue
            lit = ""
            for src in ((raw if isinstance(raw, dict) else {}),
                        (raw.get("payload") if isinstance(raw, dict) and isinstance(raw.get("payload"), dict) else {})):
                for k in ("topic", "type", "symbol", "stream"):
                    if k in src and isinstance(src[k], (str, int, float)):
                        lit += f" [{k}={src[k]}]"
            s = json.dumps(skel(raw), ensure_ascii=False) + lit
            hist[s] += 1
            first.setdefault(s, raw if not isinstance(raw, list) else raw[0])
            n += 1
            if n >= a.maxlines:
                break
        if n >= a.maxlines:
            break
    print(f"# probe {a.stream}/{a.day}: {n} строк из {min(a.tail, len(files))} файлов\n")
    for shape, cnt in hist.most_common(12):
        sep = shape.index("  [") if "  [" in shape else len(shape)
        print(f"[{cnt:>7}x] {shape[:200] if sep>200 else shape[:sep]}{shape[sep:]}")
        print("      пример:", json.dumps(first[shape], ensure_ascii=False)[:400], "\n")


# ---------------------------------------------------------------- stats

def cmd_stats(a):
    """Быстро и без скачивания: полнота сетки и байты — из имён файлов и листинга."""
    import datetime as dt
    print(f"{'поток':<9}{'день':<10}{'файлов':>8}{'ожидаемо':>9}{'размер':>11}  span, ч   примечание")
    for st in STREAMS:
        for d in a.days:
            try:
                files = day_files(a.bucket, a.prefix, st, d)
            except SystemExit:
                files = []
            if not files:
                print(f"{st:<9}{d:<10}{0:>8}{'—':>9}")
                continue
            total = sum(sz for _, sz in files)
            def hms(name):
                m = re.search(r"_(\d{8})_(\d{6})\.jsonl\.gz$", name)
                return dt.datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S") if m else None
            a0, a1 = hms(files[0][0]), hms(files[-1][0])
            span = (a1 - a0).total_seconds() / 3600 if a0 and a1 else float("nan")
            exp = int(span * 3600 / (a.rotate_min * 60)) + 1 if span == span else 0
            note = "" if len(files) >= exp else f"недобор {exp - len(files)} частей"
            print(f"{st:<9}{d:<10}{len(files):>8}{exp:>9}{total/1e9:>9.2f}GB  {span:6.1f}   {note}")


# ---------------------------------------------------------------- time series

def load_binance_series(a, days, sym):
    """[(ts_s, price, vol)] из наших binance-записей: aggTrade (осн.) + depth5 (mid).

    Возвращает отсортированный ряд; ts — время события из самого сообщения,
    если его нет — время приёма (конверт t), с пометкой не делаем: это тот же
    источник, что видит резолвер, наносекунды тут не важны."""
    want_base = to_binance_sym(sym)                      # btcusdt
    files = files_of(a, "binance", days)
    out, fallback = [], 0
    for day, f in files:
        for line in iter_lines(a.bucket, a.prefix, "binance", day, f):
            t, raw = parse_rec(line)
            if not isinstance(raw, dict) or "stream" not in raw:
                continue
            st = str(raw["stream"]).lower()
            if not st.startswith(want_base + "@"):
                continue
            d = raw.get("data") or {}
            try:
                if st.endswith("@aggtrade"):
                    px = float(d["p"]); vol = float(d.get("q", 0) or 0)
                    ts = float(d.get("T") or d.get("E") or 0) / 1000.0
                elif "@depth" in st:
                    bids = d.get("bids") or d.get("b") or []
                    asks = d.get("asks") or d.get("a") or []
                    if not bids or not asks:
                        continue
                    px = (float(bids[0][0]) + float(asks[0][0])) / 2.0
                    vol = 0.0
                    ts = float(d.get("T") or d.get("E") or 0) / 1000.0
                else:            # kline_1m и прочее — не для потока цен
                    continue
                if not ts:
                    ts = (t or 0) / 1e9
                    fallback += 1
                if ts > 1e9:
                    out.append((ts, px, vol))
            except Exception:
                pass
    out.sort(key=lambda x: x[0])
    return out, fallback


def load_chainlink(a, days, sym):
    """[(ts_event_s, price, ts_recv_s)] из rtds + (реестр топиков, кол-во строк).

    Реальные RTDS-сообщения: батчи {"topic":?,"type":?,"payload":{"symbol","timestamp",
    "data":[{"timestamp","value"},…]}}. Точное имя топика для chainlink мы угадать не
    обязаны: сопоставляем БАЗУ символа (btc/usd, BTC/USD, btcusdt, BTCUSDT -> "btc"),
    а в приоритете строки, где topic/type содержит 'chainlink'. Если их нет — берём
    все батчи этого символа (и честно сообщаем в реестре топиков, что видели)."""
    files = files_of(a, "rtds", days)
    def base(x):
        x = str(x).lower().replace("/", "")
        return re.sub(r"(usdt|usdc|usd)$", "", x)
    want = base(sym)
    rows, topics, syms, scanned = [], Counter(), Counter(), 0
    for day, f in files:
        for line in iter_lines(a.bucket, a.prefix, "rtds", day, f):
            t, raw = parse_rec(line)
            if not isinstance(raw, dict):
                continue
            scanned += 1
            pay = raw.get("payload") if isinstance(raw.get("payload"), dict) else raw
            if not isinstance(pay, dict):
                continue
            topic = str(pay.get("topic", raw.get("topic", "?")))
            typ = str(pay.get("type", raw.get("type", "?")))
            symv = str(pay.get("symbol", raw.get("symbol", "?")))
            syms[symv] += 1
            if base(symv) != want:
                continue
            topics[f"{topic} / {typ}"] += 1
            cl = "chainlink" in (topic + typ).lower()
            slash = "/" in symv                          # «ETH/USD» = оракул; «ETHUSDT» = relay Binance
            recv = (t or 0) / 1e9
            def norm(ms):
                v = float(ms)
                return v / 1000.0 if v > 1e12 else v
            pts = pay.get("data")
            batch_ts = norm(pay.get("timestamp", raw.get("timestamp", 0)))
            if isinstance(pts, list) and pts and isinstance(pts[0], dict):
                for pt in pts:
                    try:
                        rows.append((1 if cl else (2 if slash else 3),
                                     norm(pt.get("timestamp", batch_ts)),
                                     float(pt.get("value", pt.get("price"))), recv))
                    except Exception:
                        pass
            elif pay.get("value") is not None or pay.get("price") is not None:
                try:
                    rows.append((1 if cl else (2 if slash else 3),
                                 batch_ts, float(pay.get("value", pay.get("price"))), recv))
                except Exception:
                    pass
    pick = {1: [r for r in rows if r[0] == 1], 2: [r for r in rows if r[0] in (1, 2)]}
    chosen = pick[1] if pick[1] else (pick[2] if pick[2] else rows)
    series = sorted(((ts, px, rc) for _, ts, px, rc in chosen), key=lambda x: x[0])
    src = "chainlink-topic" if pick[1] else ("symbol-only(slash)" if pick[2] else ("none" if not rows else "ALL(suspect!)"))
    info = dict(topics=dict(topics), symbols=dict(syms.most_common(8)),
                rows_match=len(rows), rows_scanned=scanned, source=src)
    return series, info


def bisect_left(arr, x, key):
    lo, hi = 0, len(arr)
    while lo < hi:
        m = (lo + hi) // 2
        if key(arr[m]) < x:
            lo = m + 1
        else:
            hi = m
    return lo


def nearest(series, ts):
    """Значение в ближайший по времени момент (или None)."""
    if not series:
        return None
    i = bisect_left(series, ts, lambda p: p[0])
    cands = [j for j in (i - 1, i) if 0 <= j < len(series)]
    best = min(cands, key=lambda j: abs(series[j][0] - ts)) if cands else None
    return series[best] if best is not None else None


def q(vals, p):
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(p * len(vals)))] if vals else None


def fmt(v, nd=2, suffix=""):
    return f"{v:.{nd}f}{suffix}" if isinstance(v, (int, float)) else "—"


# ---------------------------------------------------------------- lag

def cmd_lag(a):
    days = days_of(a)
    bn_s, fb = load_binance_series(a, days, a.sym)   # btc/usd -> btcusdt внутри
    bn = [(ts, px) for ts, px, _ in bn_s]
    cl, info = load_chainlink(a, days, a.sym)
    print(f"# lag {a.sym}: binance-записей {len(bn)} (ts по событию; из приёма {fb}), "
          f"chainlink {len(cl)} (источник: {info['source']}; файлов по {a.files})")
    if info["rows_scanned"] and not info["rows_match"]:
        print(f"  в rtds нет строк для {a.sym}; символы, которые есть: {info['symbols']}; "
              f"топики: {info['topics']}")
    elif 0 < len(cl) < 60:
        print(f"  внимание: выборка Chainlink мала ({len(cl)} точек) — расширь --days (напр. "
              f"--days 20260904-20260907) и поставь --files 0 (все файлы дня)")
    if not cl or not bn:
        print("одного из рядов нет — пришли вывод этой шапки (я допишу матчинг под реальные имена)")
        return
    lo, hi = max(bn[0][0], cl[0][0]), min(bn[-1][0], cl[-1][0])
    cl = [x for x in cl if lo <= x[0] <= hi]
    bn = [x for x in bn if lo - 5 <= x[0] <= hi + 5]
    print(f"общее окно: {(hi - lo)/60:.0f} мин\n")

    # 1) насколько «свежая» цена в самом Chainlink (ts события vs реальная цена тогда)
    err_event, err_recv, age = [], [], []
    for ts, px, rts in cl:
        nb = nearest(bn, ts)
        nb2 = nearest(bn, rts if rts else ts)
        if nb:
            err_event.append(abs(px - nb[1]) / nb[1] * 1e4)      # б.п.
        if nb2:
            err_recv.append(abs(px - nb2[1]) / nb2[1] * 1e4)
        if rts:
            age.append(rts - ts)
    def line(name, vals, unit):
        print(f"{name:<44} n={len(vals):<6} медиана {fmt(statistics.median(vals) if vals else None, 2, unit)}"
              f"   p90 {fmt(q(vals, 0.9), 2, unit)}")
    line("|chainlink − mid(binance в метку времени)| [б.п.]", err_event, " б.п.")
    line("|chainlink − mid(binance в момент приёма)|  [б.п.]", err_recv, " б.п.")
    line("возраст цены в chainlink-сообщении [с]", age, " с")

    # 2) «время доезда» движения: binance сдвинулся >порога — когда то же самое
    #    показал Chainlink (первое касание нового уровня с той же стороны)?
    thr_bps = a.thresh
    arrives = []
    j = 0
    for k in range(1, len(bn)):
        t0, p0 = bn[k - 1]
        t1, p1 = bn[k]
        moved = (p1 - p0) / p0 * 1e4
        if abs(moved) < thr_bps:
            continue
        target = p1
        j = max(j, bisect_left(cl, t0, lambda c: c[0]))
        found = None
        for ci in range(j, len(cl)):
            ct, cp, crts = cl[ci]
            if ct > t1 + 60:
                break
            if moved > 0 and cp >= target * (1 - thr_bps * 1e-4 / 2):
                found = ct - t1
                break
            if moved < 0 and cp <= target * (1 + thr_bps * 1e-4 / 2):
                found = ct - t1
                break
        if found is not None and found >= 0:
            arrives.append(found)
    print(f"\nдвижений binance >{thr_bps} б.п. взято: {len(arrives)} доехавших")
    if arrives:
        print(f"время доезда до chainlink: медиана {fmt(statistics.median(arrives))} с, "
              f"p50 {fmt(q(arrives, 0.5))} с, p90 {fmt(q(arrives, 0.9))} с, "
              f"макс {fmt(max(arrives))} с")
    print("\nКак читать: «возраст» — чем Chainlink сам запаздывает внутри себя;")
    print("«ошибка в метку» — это шум/неточность, «в момент приёма» — ошибка, которую")
    print("видит резолвер рынка (на неё и ориентируемся).")


# ---------------------------------------------------------------- candles / verify

def load_last_trades(a, days, token_ids):
    """[(ts_s, price, size)] из clob last_trade для заданных asset_id."""
    out = []
    want = set(str(t) for t in token_ids) if token_ids else None
    for day, f in files_of(a, "clob", days):
        for line in iter_lines(a.bucket, a.prefix, "clob", day, f):
            _, raw = parse_rec(line)
            if isinstance(raw, list):
                msgs = raw
            elif isinstance(raw, dict):
                msgs = [raw]
            else:
                continue
            for m in msgs:
                if not isinstance(m, dict) or m.get("event_type") != "last_trade":
                    continue
                if want and str(m.get("asset_id", "")) not in want:
                    continue
                try:
                    ts = float(m.get("timestamp", 0))
                    if ts > 1e12:
                        ts /= 1000.0
                    out.append((ts, float(m["price"]), float(m.get("size", 0))))
                except Exception:
                    pass
    out.sort(key=lambda x: x[0])
    return out


def bars_from_trades(trades, tf=60):
    bars = {}
    for ts, px, sz in trades:
        k = int(ts // tf)
        b = bars.get(k)
        if not b:
            bars[k] = [px, px, px, px, sz]
        else:
            b[1] = max(b[1], px); b[2] = min(b[2], px); b[3] = px; b[4] += sz
    return [(k, *v) for k, v in sorted(bars.items())]


def bars_from_mid(series, tf=60):
    out = {}
    for ts, v in series:
        k = int(ts // tf)
        cur = out.get(k)
        if cur is None:
            out[k] = v
    ks = sorted(out)
    return [(k, v, v, v, v, 0) for k, v in ((k, out[k]) for k in ks)]


def cmd_candles(a):
    sym = a.sym if "/" in a.sym else a.sym
    a_days = days_of(a)
    if a.tokens:
        ids = a.tokens.split(",")
        tr = load_last_trades(a, a_days, ids)
        bs = bars_from_trades(tr, a.tf)
        src = f"clob last_trade ({len(tr)} сделок)"
    else:
        bn, _fb = load_binance_series(a, a_days, sym)
        bs = bars_from_trades([(ts, px, v) for ts, px, v in bn if v > 0], a.tf)
        src = f"binance aggTrade ({len(bn)} событий, {len(bs)} баров по сделкам)"
    print(f"# {sym} {a.tf}s-бары из {src}: {len(bs)} баров")
    print("first:", bs[:2])
    print("last: ", bs[-2:])
    out = f"/tmp/candles_{sym.replace('/', '-')}_{a.day}.csv"
    with open(out, "w") as f:
        f.write("t0_s,open,high,low,close,volume\n")
        for k, o, h, l, c, v in bs:
            f.write(f"{k*a.tf},{o},{h},{l},{c},{v}\n")
    print("csv:", out)


def cmd_verify(a):
    """Сверка наших 1m-свечей (из тиков) с официальными klines Binance."""
    sym = a.sym.replace("/", "").upper() + ("USDT" if "USDT" not in a.sym.upper() else "")
    url = (f"https://api.binance.com/api/v3/klines?symbol={sym}&interval=1m"
           f"&limit={min(a.files * 96, 1000)}")
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            kl = json.loads(r.read())
    except Exception as e:
        raise SystemExit(f"не смог взять официальные klines ({e}) — если сеть Binance "
                         f"блокирует AWS-IP, это ограничение региона, не наших данных")
    if not kl:
        raise SystemExit("klines пустые")
    t_from = int(kl[0][0] // 1000 // 60 * 60)
    import datetime as _dt
    vday = _dt.datetime.fromtimestamp(t_from, _dt.timezone.utc).strftime("%Y%m%d")
    bn_s, _fb = load_binance_series(a, [vday], sym)
    bn = [(ts, px) for ts, px, _ in bn_s]
    ours = dict((int(ts // 60) * 60, v) for ts, v in bn)
    diffs = []
    n = 0
    for row in kl:
        k = int(row[0] // 1000 // 60 * 60)
        mid_theirs = (float(row[2]) + float(row[3])) / 2  # (high+low)/2 — верхняя граница смысла
        if k in ours:
            diffs.append(abs(ours[k] - mid_theirs) / mid_theirs * 1e4)
            n += 1
    print(f"# verify {sym}: сверил {n} минутных слотов (наш mid vs (high+low)/2 klines)")
    if diffs:
        diffs.sort()
        print(f"медиана расхождений {statistics.median(diffs):.1f} б.п., p90 {q(diffs,0.9):.1f} б.п., "
              f"макс {diffs[-1]:.1f} б.п.")
        print("(mid-книги и high/low — разные величины; важны хвосты: если p90 единичные б.п. — "
              "наши тики покрывают Binance полностью)")
    else:
        print("нет пересечения по времени — выбери --day, где точно есть данные, и уменьши шум: "
              "klines берутся за последние минуты, а логи уходят в S3 ротацией 15м")


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["probe", "stats", "lag", "candles", "verify"])
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="kronolog")
    ap.add_argument("--day", help="YYYYMMDD (UTC)")
    ap.add_argument("--days", help="для stats: 20260904,20260905,... или '20260903-20260907'")
    ap.add_argument("--stream", default="rtds")
    ap.add_argument("--sym", default="btc/usd", help="для lag/candles: btc/usd или btcusdt")
    ap.add_argument("--tokens", help="список asset_id через запятую (для clob-свечей)")
    ap.add_argument("--tf", type=int, default=60, help="период свечей, сек")
    ap.add_argument("--files", type=int, default=8, help="сколько последних частей потока читать")
    ap.add_argument("--tail", type=int, default=2, help="probe: файлов с хвоста")
    ap.add_argument("--rotate-min", type=int, default=15, help="stats: ожидаемый период ротации")
    ap.add_argument("--maxlines", type=int, default=20000, help="probe: максимум строк")
    ap.add_argument("--thresh", type=float, default=5.0, help="lag: порог движения, б.п.")
    a = ap.parse_args()

    if a.mode == "stats":
        if a.days and "-" in a.days:
            x, y = a.days.split("-")
            import datetime as dt
            d0 = dt.datetime.strptime(x, "%Y%m%d")
            d1 = dt.datetime.strptime(y, "%Y%m%d")
            a.days = [(d0 + dt.timedelta(d)).strftime("%Y%m%d") for d in range((d1 - d0).days + 1)]
        else:
            a.days = (a.days or a.day or "").split(",")
        cmd_stats(a)
        return
    if not (a.day or a.days) and a.mode != "verify":
        raise SystemExit("нужен --day YYYYMMDD или --days A-B")
    if not a.day and a.days:
        a.day = days_of(a)[0]
    {"probe": cmd_probe, "lag": cmd_lag, "candles": cmd_candles, "verify": cmd_verify}[a.mode](a)


if __name__ == "__main__":
    main()
