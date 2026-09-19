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
import gzip
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
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


def unescape(ln: bytes) -> bytes:
    """Снять экранирование с тела — ТОЛЬКО когда оно реально есть.

    Формат строки логгера (kronolog.py:140 `write_raw`) зависит от содержимого:
        {"t":<ns>,"raw":{…}}          нормальный WS-json — компактный, БЕЗ экранирования
        {"t":<ns>,"text":"{\n…}"}     нестандартный/многострочный payload — экранирован
    20.09 я объявил причиной «пустого датасета» второе и написал replace на каждую
    строку; проба на живом логе (08.09, 300 000 строк -> 24 asset_id) показала, что
    clob-поток пишет первое, а настоящий убийца был `| zcat` в старом iter_lines — он
    отдавал НОЛЬ СТРОК, и это неотличимо было от «лог пустой». replace оставлен
    ровно для fallback-конверта и сделан условным: на 8.8ГБ/сутки он иначе стоил бы
    аллокацией на строку при нулевой пользе.
    """
    return ln.replace(b'\\"', b'"') if b'\\"' in ln else ln


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
    """Строки одного jsonl.gz-объекта, без скачивания на диск и БЕЗ внешнего zcat.

    Декодирует python-gzip поверх трубы: `... | zcat` с stderr в DEVNULL на боксе, где
    zcat нет (или он отказывается читать из pipe), выглядит ровно как пустой объект —
    «333 файла прочитано, 0 строк, 0 токенов», и мы дважды потратили на это прогон.
    Байты за байтами: regex-и в вызывающем коде работают по bytes, поэтому здесь нет
    decode/encode на строку (50 млн строк в сутки это стоило бы минут 10 CPU).
    """
    uri = f"s3://{bucket}/{prefix}/{stream}/{day}/{name}"
    p = subprocess.Popen(f"aws s3 cp '{uri}' - --only-show-errors", shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    gz = gzip.GzipFile(fileobj=p.stdout)
    buf = b""
    while True:
        try:
            chunk = gz.read(1 << 20)
        except OSError as e:                    # битый gzip/обрыв = НЕ «пусто»
            p.kill()
            raise SystemExit(f"[{day}/{stream}] поток оборвался на {name}: {e!r}")
        if not chunk:
            break
        buf += chunk
        parts = buf.split(b"\n")
        buf = parts.pop()
        for x in parts:
            if x:
                yield unescape(x)
    code = p.wait()
    if code != 0:
        raise SystemExit(f"[{day}/{stream}] aws s3 cp вернул rc={code} для {uri} — это "
                         "читаемость объекта (роль/регион/ключ), а НЕ «пустой лог»")
    if buf:
        yield unescape(buf)


HDRS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) kronos-research/1.0",
        "Accept": "application/json"}


def _mkinfo(mk):
    slug = mk.get("slug") or ""
    s = SLUG_RE.match(slug)
    if not s or s.group(2) not in CODE_S:
        return
    toks = mk.get("clobTokenIds")
    outs = mk.get("outcomes")
    try:
        toks = json.loads(toks) if isinstance(toks, str) else (toks or [])
        outs = json.loads(outs) if isinstance(outs, str) else (outs or [])
        up = outs.index("Up")
    except Exception:
        up = 0
    for k, t in enumerate(toks):
        yield str(t), {"slug": slug, "asset": s.group(1), "code": s.group(2),
                       "start": int(s.group(3)), "up": (k == up)}


def gamma_map(ids, gamma, batch=8, threads=8):
    """token-id -> {slug, asset, code, start, up}.

    Gamma на некоторых IP/пулах отвергает длинные запросы (422 на батч из 40
    id, при этом поштучно 200), поэтому перебираем форматы: повторяющийся
    параметр, запятые по 20/5, поштучно нитями. Формат, разметивший <50%
    или вернувший 4xx, считается непригодным — идём дальше по остатку."""
    ids = sorted(set(str(x) for x in ids))
    m = {}

    def fetch(url):
        for att in range(3):
            try:
                req = urllib.request.Request(url, headers=HDRS)
                with urllib.request.urlopen(req, timeout=30) as r:
                    return json.loads(r.read().decode()), None
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 502, 503):
                    time.sleep(1.5 * (att + 1))
                    continue
                return None, e.code
            except Exception:
                return None, "net"
        return None, "retry"

    def absorb(arr):
        for mk in arr or []:
            for t, v in _mkinfo(mk):
                m[t] = v

    # закрытые/архивные рынки Gamma по умолчанию НЕ отдаёт ([] на все исторические
    # окна — проверено 14.09 с расчётной машины), поэтому доклеиваем флаги.
    gp = "" if "?" in gamma else "&closed=true&archived=true"

    def multi(ch):
        return gamma + "/markets?" + "&".join("clob_token_ids=" + t for t in ch) + gp

    def comma(ch):
        return gamma + "/markets?clob_token_ids=" + ",".join(ch) + gp

    for name, size, url_of in [("multi", max(1, batch), multi), ("comma", 20, comma),
                               ("comma", 5, comma), ("single", 1, comma)]:
        ts = [t for t in ids if t not in m]
        if not ts:
            break
        before = len(m)
        chunks = [ts[i:i + size] for i in range(0, len(ts), size)]
        npar = threads if size == 1 else 1
        print(f"  gamma/{name}: {len(ts)} id пачками по {size} (~{len(chunks)} "
              f"запросов, нитей {npar})", flush=True)
        rejected, errs, err = False, 0, None
        if npar > 1:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=npar) as ex:
                for j, (arr, err) in enumerate(ex.map(
                        lambda ch: fetch(url_of(ch)), chunks)):
                    if err == 403:
                        print("  gamma: 403 — IP отрезан; останавливаюсь",
                              file=sys.stderr)
                        return m
                    if err:
                        errs += 1
                        if errs > max(30, len(chunks) // 5):
                            rejected = True
                            break
                    else:
                        absorb(arr)
                    if j and j % 2000 == 0:
                        print(f"  gamma/{name}: размечено {len(m)}/{len(ids)}",
                              flush=True)
        else:
            for j, ch in enumerate(chunks):
                arr, err = fetch(url_of(ch))
                if err == 403:
                    print("  gamma: 403 — IP отрезан; останавливаюсь",
                          file=sys.stderr)
                    return m
                if err:
                    if size > 1 and err in (400, 414, 422):
                        rejected = True
                        break
                    errs += 1
                    if errs > max(30, len(chunks) // 5):
                        rejected = True
                        break
                else:
                    absorb(arr)
                if j and j % 200 == 0:
                    print(f"  gamma/{name}: размечено {len(m)}/{len(ids)}",
                          flush=True)
        cov = (len(m) - before) / max(1, len(ts))
        if rejected:
            print(f"  gamma/{name}: формат не принят (первая ошибка {err}); "
                  f"пробую мельче", flush=True)
        elif cov < 0.5:
            print(f"  gamma/{name}: разметил лишь {100*cov:.0f}% — пробую дальше",
                  flush=True)
        elif cov >= 1:
            break
    print(f"  gamma: итог размечено {len(m)} из {len(ids)}", flush=True)
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
                w = wins[key] = {"slot": {}, "pend": cps, "prev": (None,) * 6,
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
                w["prev"] = freeze(w["bids"], w["asks"], a.depth_cents / 100.0)
    for w in wins.values():                               # остаток хвостов: текущее состояние
        while w["pend"]:
            c = w["pend"].pop(0)
            w["slot"][c] = w["prev"]
    print(f"  [{day}/passC] строк {n_lines:,}; размечено {matched:,}; окон {len(wins)}", flush=True)
    if not wins:
        # Именно этот случай и прошёл незамеченным: n_lines>0 (стрим работал), а
        # matched=0 — карты не пересеклись с логом. Записать такую сутки как
        # «готовые» = подарить всем последующим прогонам пустой датасет.
        raise SystemExit(f"[{day}/passC] 0 окон при {n_lines:,} прочитанных строк и "
                         f"{len(tokmap):,} токенов в карте — несовпадение карты с "
                         f"логом (или n_lines=0: стрим не отдаёт ничего, проверь "
                         f"регион/профиль: aws s3 ls s3://{a.bucket}/{a.prefix}/clob/"
                         f"{day}/ | head -2)")
    out = os.path.join(a.outdir, f"windows_{day}.csv")
    cp_all = sorted(set(CPS["5m"]) | set(CPS["15m"]) | set(CPS["4h"]), reverse=True)
    # .tmp + os.replace: сутки помечаются готовыми по наличию непустого файла, и без
    # атомарности kill/обрыв S3 на ползаписи оставил бы урезанный датасет под именем
    # готового — все прогоны честно посчитали бы его и ничего бы не заметили.
    with open(out + ".tmp", "w", newline="") as f:
        wc = csv.writer(f)
        head = ["start", "end", "asset", "code", "n_book", "n_pc", "n_lt", "vol_usd"]
        for c in cp_all:
            head += [f"b{c}", f"a{c}", f"bs{c}", f"as{c}", f"d2b{c}", f"d2a{c}"]
        wc.writerow(head)
        for (start, asset, code), w in sorted(wins.items()):
            row = [start, w["end"], asset, code, w["n_book"], w["n_pc"], w["n_lt"], round(w["vol"], 1)]
            for c in cp_all:
                v = w["slot"].get(c, (None,) * 6)
                row += ["" if x is None else x for x in v]
            wc.writerow(row)
        f.flush()
        os.fsync(f.fileno())
    os.replace(out + ".tmp", out)
    print(f"  [{day}] -> {out}", flush=True)


def freeze(bids, asks, dcent=0.02):
    """Снимок топа Up-токена: (bid, ask, sz_bid, sz_ask, d2_bid, d2_ask).

    Размеры — В АКЦИЯХ (так их отдаёт CLOB), не в долларах. d2_* — суммарный
    выставленный объём в пределах dcent от топа с каждой стороны. Это единственное,
    что в датасете отличает «цена была» от «там было чем торговать», и по своей
    природе ВЕРХНЯЯ граница ёмкости: мы встаём в очередь ЗА этим объёмом, а не
    получаем его; объём агрессора, снявшего наш уровень, из снапшотов не виден
    вообще (это уже M6 — пересечения между t1/t2 по сырому логу).

    Нужно затем, чтобы не стримить 8.8 ГБ/сутки вторично: M5 (цены), тейкер-
    исследование (исполнимость) и M6 (ёмкость) строятся по одному файлу.
    """
    b = max(bids); a = min(asks)
    return (round(b, 4), round(a, 4), round(bids[b], 2), round(asks[a], 2),
            round(sum(v for q, v in bids.items() if q >= b - dcent), 2),
            round(sum(v for q, v in asks.items() if q <= a + dcent), 2))


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
    with open(out + ".tmp", "w", newline="") as f:
        wc = csv.writer(f)
        wc.writerow(["day", "minute", "asset", "close"])
        for base, d in sorted(rows.items()):
            for m, px in sorted(d.items()):
                wc.writerow([day, m, base, px])
        f.flush()
        os.fsync(f.fileno())
    os.replace(out + ".tmp", out)
    print(f"  [{day}/minutes] aggTrade {n:,} -> {out}", flush=True)


def _collect_worker(args):
    bucket, prefix, day, outdir, force = args
    idf = os.path.join(outdir, f"ids_{day}.txt")
    if force and os.path.exists(idf):
        os.remove(idf)
    if os.path.exists(idf):
        return day, set(open(idf).read().split())
    files = day_files(bucket, prefix, "clob", day)
    if not files:
        return day, set()
    seen, nf, nlines, first = set(), len(files), 0, b""
    for fi, name in enumerate(files):
        for ln in iter_lines(bucket, prefix, "clob", day, name):
            nlines += 1
            if not first:
                first = ln[:180]
            m = RX_ID.search(ln)
            if m:
                seen.add(m.group(1).decode())
    if not seen:
        # 20.09: 187 файлов «прочитано», 0 токенов, и ровно это дало датасет из одной
        # шапки на 13 суток. Разница между «труба ничего не принесла» и «принесла, но
        # не то» стоит полчаса разговора, поэтому она печатается, а не догадывается.
        probe = (f"aws s3 cp s3://{bucket}/{prefix}/clob/{day}/{files[0]} - | zcat "
                 "| head -2")
        if not nlines:
            raise SystemExit(f"[{day}/passA] {nf} файлов, 0 ПРОЧИТАННЫХ СТРОК — "
                             f"труба пустая (aws/zcat/регион). Проверь на боксе: {probe}")
        raise SystemExit(f"[{day}/passA] {nlines:,} строк прочитано, 0 совпадений с "
                         f"RX_ID (asset_id). Первая строка потока: {first!r}")
        if fi % 20 == 19 or fi + 1 == nf:
            print(f"  [{day}/passA] файл {fi+1}/{nf}, токенов {len(seen)}",
                  flush=True)
    with open(idf, "w") as f:
        f.write("\n".join(sorted(seen)))
    return day, seen


def _run_worker(args):
    # Кортеж и Namespace здесь — ЕДИНСТВЕННЫЙ способ, каким a.depth_cents доходит до
    # run_day: в воркере пула живёт ns, собранный руками, а не парсер main(). Забыть
    # в нём ключ = AttributeError после 8.8ГБ стрима (20.09, passC). Держит это
    # tests/test_clobwin_args.py — и статически, и вызовом _run_worker как пул.
    bucket, prefix, day, outdir, tokmap, minutes, assets, depth_cents = args
    ns = argparse.Namespace(bucket=bucket, prefix=prefix, outdir=outdir,
                            depth_cents=depth_cents)
    files = day_files(bucket, prefix, "clob", day)
    if files:
        run_day(ns, day, tokmap)
    if minutes:
        do_minutes(ns, day, set(assets))
    return day


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="kronolog")
    ap.add_argument("--day")
    ap.add_argument("--days", help="A-B или список")
    ap.add_argument("--outdir", default="/tmp/win")
    ap.add_argument("--gamma", default="https://gamma-api.polymarket.com")
    ap.add_argument("--gamma-batch", type=int, default=8,
                    help="старт. размер батча повторяющегося параметра Gamma")
    ap.add_argument("--gamma-threads", type=int, default=8,
                    help="нитей в поштучном фолбэке Gamma")
    ap.add_argument("--minutes", action="store_true")
    ap.add_argument("--jobs", type=int, default=1, help="0=все ядра; параллелизм ПО СУТКАМ")
    ap.add_argument("--map-only", action="store_true",
                    help="только passA+gamma: собрать карту токенов и залить в --map-s3 (режим сервера-логгера)")
    ap.add_argument("--map-s3", default="", help="s3-ключ карты (загрузка/выгрузка), напр. kronos/tokens_map.json")
    ap.add_argument("--rescan", action="store_true", help="принудительно собрать ids заново, даже если карта есть")
    ap.add_argument("--map-fill", action="store_true",
                    help="режим сервера-логгера: скачать ids_unmapped из S3, спросить Gamma, залить карту")
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    ap.add_argument("--depth-cents", type=float, default=2.0,
                    help="окно глубины в центах для d2b/d2a (0 = только топы)")
    a = ap.parse_args()
    days = []
    if a.day:
        days = [a.day]
    elif a.days:
        # список с диапазонами: «20260914,20260917-20260918» — рваные сутки
        # (15–16.09) приходится вычеркивать именно так, а не «взять размах и
        # замазать дыру»: пропуск суток должен быть виден в выводе по числу дней.
        for part in a.days.split(","):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                x, y = part.split("-", 1)
                d0 = dt.datetime.strptime(x.strip(), "%Y%m%d")
                d1 = dt.datetime.strptime(y.strip(), "%Y%m%d")
                while d0 <= d1:
                    days.append(d0.strftime("%Y%m%d")); d0 += dt.timedelta(days=1)
            else:
                days.append(part)
        days = sorted(set(days))
    if a.day or a.days:
        # Один не-день в списке раньше превращался в windows_auto.csv и в «0 строк»
        # без единой жалобы; пусть лучше упадёт здесь, до стрима 8.8ГБ/сутки.
        bad = [d for d in days if len(d) != 8 or not d.isdigit()]
        if bad:
            raise SystemExit(f"не-сутки в --day/--days: {bad} (формат 20260914)")
    os.makedirs(a.outdir, exist_ok=True)
    map_path = os.path.join(a.outdir, "tokens_map.json")
    if a.map_s3 and (not os.path.exists(map_path) or a.rescan):
        r = subprocess.run(f"aws s3 cp s3://{a.bucket}/{a.map_s3} {map_path}",
                           shell=True, capture_output=True)
        if r.returncode != 0:
            # Раньше этот cp ошибался молча, а дальше шёл `tokmap = json.load(...)`
            # над ЧУЖИМ файлом прошлой попытки (или пустым), и сутки собирались в
            # шапку без единого окна — при exit code 0. Ключ карты — единственное,
            # без чего пассы бессмысленны, поэтому его отсутствие говорим вслух.
            print(f"  карта из s3://{a.bucket}/{a.map_s3} не скачалась "
                  f"({(r.stderr or b'').decode()[:120].strip() or 'ключа нет'}); "
                  f"local map_path={'есть' if os.path.exists(map_path) else 'нет'} — "
                  f"ids соберём с нуля через Gamma", flush=True)
    cov_path = os.path.join(a.outdir, "map_covered.json")
    if a.map_s3 and not os.path.exists(cov_path):
        subprocess.run(f"aws s3 cp s3://{a.bucket}/{a.map_s3}.days {cov_path}",
                       shell=True, capture_output=True)
    covered = set()
    if os.path.exists(cov_path) and not a.rescan:
        try:
            covered = set(json.load(open(cov_path)))
        except Exception:
            covered = set()
    tokmap = {}
    # Карту читаем ВСЕГДА, если она есть: --rescan = «пересобрать ids и переспросить
    # Gamma по новым токенам», а не «забыть 6 МБ уже известного». Раньше при --rescan
    # tokmap оставался пустым, и если passA к тому же не нашёл ни одного id, падало
    # «карта токенов пуста» — честная, но ложная формулировка при лежащей рядом карте.
    if os.path.exists(map_path):
        try:
            tokmap = json.load(open(map_path))
            print(f"карта токенов: {len(tokmap)} (файл), покрытые сутки: "
                  f"{sorted(covered)[:3]}…{sorted(covered)[-2:] if len(covered) > 2 else ''}")
        except Exception:
            tokmap = {}
    jobs = a.jobs or (os.cpu_count() or 2)

    if a.map_only:                        # режим сервера-логгера: карта -> S3
        import multiprocessing as mp
        with mp.Pool(min(jobs, max(1, len(days)))) as pool:
            seen = set()
            for day, ids in pool.imap_unordered(_collect_worker,
                                                 [(a.bucket, a.prefix, d, a.outdir,
                                                   a.rescan) for d in days]):
                print(f"  [{day}/passA] токенов {len(ids)}", flush=True)
                seen |= ids
        fresh = [x for x in seen if x not in tokmap]
        if fresh:
            tokmap.update(gamma_map(fresh, a.gamma, a.gamma_batch, a.gamma_threads))
        if fresh and not tokmap:
            raise SystemExit("Gamma не отвечает с этого IP — проверь curl -w %{http_code}")
        json.dump(tokmap, open(map_path, "w"))
        print(f"карта: {len(tokmap)} токенов")
        if a.map_s3:
            subprocess.run(f"aws s3 cp {map_path} s3://{a.bucket}/{a.map_s3}", shell=True)
            print("карта залита в s3://" + a.bucket + "/" + a.map_s3)
        return

    if a.map_fill:                        # лёгкая роль: только Gamma по чужому списку ids
        ids_path = map_path + ".ids"
        if a.map_s3:
            subprocess.run(f"aws s3 cp s3://{a.bucket}/{a.map_s3}.ids {ids_path}",
                           shell=True, capture_output=True)
        ids = [x.strip() for x in open(ids_path)] if os.path.exists(ids_path) else []
        if not ids:
            raise SystemExit("нет ids_unmapped — сначала запуска clobwin на расчётной машине")
        fresh = [x for x in ids if x not in tokmap]
        tokmap.update(gamma_map(fresh, a.gamma, a.gamma_batch, a.gamma_threads))
        json.dump(tokmap, open(map_path, "w"))
        if a.map_s3:
            subprocess.run(f"aws s3 cp {map_path} s3://{a.bucket}/{a.map_s3}", shell=True)
        print(f"готово: карта {len(tokmap)} токенов залита в S3")
        return

    need_ids = [d for d in days if not tokmap or d not in covered]
    if need_ids:
        import multiprocessing as mp
        with mp.Pool(min(jobs, len(need_ids))) as pool:
            seen = set()
            for day, ids in pool.imap_unordered(_collect_worker,
                                                 [(a.bucket, a.prefix, d, a.outdir,
                                                   a.rescan) for d in need_ids]):
                print(f"  [{day}/passA] токенов {len(ids)}", flush=True)
                seen |= ids
        fresh = [x for x in seen if x not in tokmap]
        if fresh:
            tokmap.update(gamma_map(fresh, a.gamma, a.gamma_batch, a.gamma_threads))
            json.dump(tokmap, open(map_path, "w"))
            if a.map_s3:
                subprocess.run(f"aws s3 cp {map_path} s3://{a.bucket}/{a.map_s3}", shell=True)
            print(f"  карта обновлена: {len(tokmap)}")
        if fresh and not tokmap:          # Gamma не отдаёт ничего — relay-сценарий
            idsp = map_path + ".ids"
            open(idsp, "w").write("\n".join(sorted(fresh)))
            if a.map_s3:
                subprocess.run(f"aws s3 cp {idsp} s3://{a.bucket}/{a.map_s3}.ids", shell=True)
                print(f"ids залиты в s3://{a.bucket}/{a.map_s3}.ids")
            raise SystemExit(
                "\nGamma недоступна с этого IP. Релей: на сервере-логгере выполни\n"
                f"  python3 clobwin.py --bucket {a.bucket} --outdir {a.outdir} "
                f"--map-fill --map-s3 {a.map_s3 or 'kronos/tokens_map.json'}\n"
                "и повтори эту команду — карта подтянется из S3.")
        # отмечаем покрытие: сутки, где ids найдены и доспрошены, — готовы;
        # пустые (логгер ещё не дописал) НЕ помечаем — переснимутся позже
        for d in need_ids:
            idf = os.path.join(a.outdir, f"ids_{d}.txt")
            if os.path.exists(idf) and os.path.getsize(idf) > 0:
                covered.add(d)
        json.dump(sorted(covered), open(cov_path, "w"))
        if a.map_s3 and covered:
            # Леджер покрытия — общее S3-состояние: бокс, у которого ids не собрались
            # (свежий калькулятор, отравленная карта), не имеет права записывать туда
            # пустой список и тем самым «обнулять» покрытие для всех остальных.
            subprocess.run(f"aws s3 cp {cov_path} s3://{a.bucket}/{a.map_s3}.days",
                           shell=True, capture_output=True)
        elif a.map_s3:
            print("  покрытие НЕ заливаю в S3: список пуст — пусть его держит бокс, "
                  "который реально собрал ids", flush=True)
    if not tokmap:
        # Пустая карта = сутки, в которых 0 размеченных книг. Раньше это выглядело
        # как успешный прогон с csv из одной шапки (20.09 на калькуляторе), и все
        # последующие шаги — venue/taker/MM — честно отрапортовали «0 окон».
        raise SystemExit(
            "карта токенов пуста — строить нечего. Либо s3-ключ карты неверен/не "
            "скачался (смотри строку «карта из s3://…» выше), либо Gamma не отдала "
            "ни одного рынка. Первичная сборка: --rescan (пересобрать ids и "
            "спросить Gamma); покрытие лежит рядом в .days и на свежем боксе ему "
            "верить нельзя — см. need_ids.")
    assets = set(x.strip().lower() for x in a.assets.split(","))
    work = [(a.bucket, a.prefix, d, a.outdir, tokmap, a.minutes, assets,
             a.depth_cents) for d in days
            if day_files(a.bucket, a.prefix, "clob", d)]
    import multiprocessing as mp
    with mp.Pool(min(jobs, max(1, len(work)))) as pool:
        for day in pool.imap_unordered(_run_worker, work):
            print(f"  [{day}] passC завершён", flush=True)
    print("готово:", a.outdir)


if __name__ == "__main__":
    main()
