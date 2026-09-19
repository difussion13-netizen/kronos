#!/usr/bin/env python3
"""markcalc.py — «проверка на калькуляторе» по НАКОПЛЕННЫМ данным, без реплея
и без нового кода в трейдере. Три числа, которые нельзя получить иначе дёшево:

A) Гейт M9 №2 задним числом: медиана markout синглов = (val − px)·100 по
   платежу разрешения — ровно формула близнеца (clobzero M2), где val считается
   по ЯРЛЫКУ live-`resolve()`: fin = close(end−1), bench = close(start).
   Это восстанавливает метрику для суток, когда событие `markout` ещё не
   писалось (18.09 и далее), — гейт не надо ждать до 23.09.
B) Насколько важна конвенция `bench`: ярлык пересчитывается вторым прогоном с
   bench = close(start−60) (как в live-`checkpoint()`) и сообщается доля
   рынков, где исход ФЛИПАЕТСЯ, + медиана |fin−bench| (в б.п.) — если флипов
   доли процента, расхождение live↔clobmm в знаменателе x неважно; если
   единицы процентов — это дефект формулы и он судится раньше любых гейтов.
C) Приближённая «правда площадки» для спора: у рынков с эндшпильным филлом
   (последний intent cp ≤ 15 c) mid книги в момент филла ≈ вера рынка.
   Сравниваем её со знаком (fin−bench) обеих конвенций и считаем, какая
   совпадает чаще (фильтр: |mid−0.5| ≥ 0.2, иначе прокси ничего не утверждает).

Данные: journal трейдера + 1m-клины Binance из кэша tfmprobe (/tmp/kl).
Память: минуты в array, поиск бинарный — ~20 МБ на символ.

  python3 markcalc.py --journal /tmp/live-journal.jsonl --cache /tmp/kl --log /tmp/live.log
  # платёж площадки по ВСЕМ окнам суток (для тейкер-исследования / H по всем окнам):
  python3 markcalc.py --venue-keys /tmp/win --venue-key-days 20260904-20260914 \
    --venue-workers 6 --venue-cache /tmp/venue.jsonl
  python3 markcalc.py ... --fetch-missing --days 12     # докачать sol/xrp
"""
import argparse
import bisect
import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request
from array import array

API = "https://api.binance.com/api/v3/klines"
# api.binance.com отвечает 451 в ряде юрисдикций; data-api.binance.vision — тот же
# публичный маркет-дата эндпоинт без аккаунта. Пробуем по очереди.
API_HOSTS = ("https://api.binance.com/api/v3/klines",
             "https://data-api.binance.vision/api/v3/klines")


def kline_rows(symbol, start_ms, end_ms, chunk=1000):
    """[ms,o,h,l,c] по [start_ms, end_ms); пустой список, если не удалось."""
    out, t = [], int(start_ms)
    while t < end_ms:
        url = None
        for host in API_HOSTS:
            url = (f"{host}?symbol={symbol}&interval=1m"
                   f"&startTime={t}&limit={chunk}")
            try:
                data = json.loads(urllib.request.urlopen(url, timeout=20).read())
                break
            except Exception as e:
                data = None
                print(f"  fetch {symbol}@{t} {host.split('/')[2]}: {e}",
                      file=sys.stderr)
        if not data:
            break
        for k in data:
            out.append([int(k[0]), float(k[1]), float(k[2]), float(k[3]),
                        float(k[4])])
        t = int(data[-1][0]) + 60000
    return out
SYM = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "xrp": "XRPUSDT"}
WIN = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
F_CENTS = 0.5            # f = 0.005 в центах: структурный пол срабатывания


def load_minutes(path, symbol, days, fetch):
    """Минуты/цены из кэша tfmprobe; при --fetch-missing докачать с Binance.

    Формат кэша: [open_ms, close] (легасия, open нет) или [open_ms, open, close].
    open нужен затем, чтобы посчитать bench/fin РОВНО на границах окна — именно так
    устроено разрешение updown-рынков (цена на start и на end), а close(c) — это
    цена на ~59 с ПОЗЖЕ границы, т.е. для bench она вбирает в себя минуту внутри
    самого окна сделки.
    """
    fn = os.path.join(path, f"{symbol}.jsonl")
    mins = array("q")
    cl, op, hi, lo = array("d"), array("d"), array("d"), array("d")
    for _ in range(0):
        pass
    have_ohlc = have_op = True
    if os.path.exists(fn) and os.path.getsize(fn) > 0:
        with open(fn) as f:
            for ln in f:
                try:
                    r = json.loads(ln)
                except Exception:
                    continue
                mins.append(int(r[0]) // 60000)
                if len(r) >= 5:                 # ms, o, h, l, c
                    op.append(float(r[1])); hi.append(float(r[2]))
                    lo.append(float(r[3])); cl.append(float(r[4]))
                elif len(r) >= 3:               # ms, o, c
                    have_ohlc = False
                    op.append(float(r[1])); cl.append(float(r[2]))
                    hi.append(0.0); lo.append(0.0)
                else:                           # ms, c (легасия: ни open, ни h/l)
                    have_ohlc = have_op = False
                    cl.append(float(r[1]))
                    op.append(0.0); hi.append(0.0); lo.append(0.0)
    if fetch and mins and os.path.exists(fn):
        # КЭШ ЕСТЬ, но он обрывается (типично: скачан под сим-окно, а живой
        # журнал ушёл на сутки вперёд). Раньше это молча съедалось reach-back'ом.
        last = int(mins[-1])
        now_m = int(time.time() * 1000) // 60000
        if now_m - last > 2:
            rows = kline_rows(symbol, (last + 1) * 60000, now_m * 60000)
            if rows:
                with open(fn, "a") as f:
                    for r in rows:
                        f.write(json.dumps(r) + "\n")
                print(f"  кэш {symbol}: дописано {len(rows)} минут "
                      f"(было до {time.strftime('%m-%d %H:%M', time.gmtime(last * 60))}, "
                      f"теперь до "
                      f"{time.strftime('%m-%d %H:%M', time.gmtime(rows[-1][0] / 1000))})",
                      file=sys.stderr)
                return load_minutes(path, symbol, days, fetch=False)
    elif fetch:
        end = int(time.time() * 1000) // 60000 * 60000
        t = end - days * 86400 * 1000
        rows = kline_rows(symbol, t, end)
        os.makedirs(path, exist_ok=True)
        with open(fn, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        print(f"  докачан {symbol}: {len(rows)} минут", file=sys.stderr)
        return load_minutes(path, symbol, days, fetch=False)
    return (mins, cl, (op if have_op else None),
            (hi if have_ohlc else None), (lo if have_ohlc else None))


def est_at(md, t, kind):
    """Цена на границе окна по выбранной оценке. Площадка берёт TWAP60, т.е.
    среднее по минуте, СОВЕРШАЮЩЕЙСЯ перед границей; our-кэшируют только 1м-свечи,
    поэтому сравниваем все разумные приближения одним же способом:
      close(t)      — закрытие минуты, содержащей t   (= mget(t); текущий сим)
      close(t-60)   — закрытие минуты ДО t            (= текущее живое решение)
      mean(t-60)    — (o+h+l+c)/4 минуты до t         ← прокси TWAP60
      mean(t)       — (o+h+l+c)/4 минуты, содержащей t
      open(t)       — цена ровно на границе t
    """
    mins, cl, op, hi, lo = md
    m = int(t) // 60
    i = bisect.bisect_left(mins, m)
    # якорь обязан быть ровно этой минутой: reach-back превращал бы «оценку
    # границы» в «последнюю цену, которая была в кэше» (см. close_at)
    if i >= len(mins) or mins[i] != m:
        return None
    if kind == "close_t":
        return cl[i]
    if kind == "close_p":
        return cl[i - 1] if i >= 1 and mins[i - 1] == m - 1 else None
    if kind == "open_t":
        return None if not op else op[i]
    if kind == "mean_t":
        return None if (not hi or hi[i] <= 0) else (op[i] + hi[i] + lo[i] + cl[i]) / 4.0
    if kind == "mean_p":
        j = i - 1
        if j < 0 or not hi or hi[j] <= 0 or mins[j] != m - 1:
            return None
        return (op[j] + hi[j] + lo[j] + cl[j]) / 4.0
    raise ValueError(kind)


def close_at(mins, cl, t):
    """Close РОВНО минуты, содержащей t; если такой минуты в кэше нет — None.

    Когда-то это было «последняя минута <= t» (дословная семантика движкового
    MinuteBook.close). Для ОФЛАЙН-ИСТИНЫ так нельзя: при дыре или обрыве кэша
    вчерашняя бар-цена выдаётся за сегодняшнюю, и это не шумы, а подлог — на
    реальных данных оно дало 44 «точных ничьих» |fin−bench| < 0.05 б.п. в B),
    «офолдеры» ровно 100¢ в A) и «ни та ни другая минута» в L) с расстояниями,
    одинаковыми до третьего знака для обеих кандидатных минут (обе кандидата
    сваливались в один и тот же последний бар).
    """
    return cl_at(mins, cl, t)[1]


def cl_at(mins, cl, t):
    m = int(t) // 60
    i = bisect.bisect_left(mins, m)
    if i < len(mins) and mins[i] == m:
        return (m, cl[i])
    return (None, None)




def fetch_payment(asset, start, code, timeout=8.0):
    """(y, ptb, fp) по ОДНОму slug через Gamma. y=None — сбоящий запрос, -1 — жив.

    Только HTTP и разбор; кэш и циклы — на вызывающем (venue_truth), чтобы и
    последовательный ритуальный прогон, и потоковый шли по одному коду разбора:
    расхождение в том, что считается «фактическим разрешением», дорого.
    """
    slug = f"{asset}-updown-{code}-{start}"
    url = ("https://gamma-api.polymarket.com/events?"
           + urllib.parse.urlencode({"slug": slug}))
    y = None
    ptb = fp = None
    for att in range(2):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "kronocalc/0.1"})
            evs = json.loads(urllib.request.urlopen(req, timeout=timeout).read())
            for ev in evs or []:
                em = ev.get("eventMetadata") or {}
                if em.get("priceToBeat") is not None:
                    ptb = float(em["priceToBeat"])
                    fp = em.get("finalPrice")
                    fp = None if fp is None else float(fp)
                for m in ev.get("markets", []):
                    op = m.get("outcomePrices")
                    if isinstance(op, str):
                        op = json.loads(op)
                    if not op or len(op) < 2:
                        continue
                    if not m.get("closed"):
                        y = -1                     # ещё жив
                    else:
                        y = 1 if float(op[0]) > 0.5 else 0
                    break
                if y is not None:
                    break
            break
        except Exception:
            time.sleep(0.7 * (att + 1))
    return y, ptb, fp


def venue_truth(cache, want, timeout=8.0, sleep=0.12, workers=1):
    """{key -> {"y","ptb","fp"}}: ФАКТИЧЕСКОЕ разрешение рынка площадкой.

    y — по outcomePrices (0/1 = Up/Down); ptb/fp — eventMetadata.priceToBeat и
    finalPrice, т.е. ТЕ САМЫЕ числа, по которым платят: rule = «Up, если TWAP(60с)
    Chainlink BTC/USD на конце окна ≥ TWAP на начале», ties ⇒ Up. Кэш resumable:
    непереснятые ключи дозапрашиваются, откаты не повторяем.
    """
    have = {}
    if os.path.exists(cache):
        with open(cache) as f:
            for ln in f:
                try:
                    r = json.loads(ln)
                except Exception:
                    continue
                have[(r["asset"], int(r["start"]))] = r
    # ключ сверки — (asset,start), но slug требует code: держим его рядом
    trip = []
    for (a, st, *_rest) in want:
        trip.append((a, int(st), (_rest[0] if _rest else "")))
    want = list(dict.fromkeys([(a, st) for (a, st, _c) in trip]))
    code_of = {}
    for (a, st, c) in trip:
        code_of[(a, st)] = c
    todo = [(a, st, code_of.get((a, st), "")) for (a, st) in
            dict.fromkeys(want) if (a, st) not in have]
    out = {k: {"y": have[k].get("y"), "ptb": have[k].get("ptb"),
               "fp": have[k].get("fp"), "code": have[k].get("code")}
           for k in want if k in have}
    if not todo:
        return out, 0
    fails = 0

    def rec(asset, start, code, y, ptb, fp):
        """Запись одного ключа в выход и в кэш. False = прекращать (сеть мертва)."""
        out[(asset, int(start))] = {"y": y, "ptb": ptb, "fp": fp, "code": code}
        # в кэш — ТОЛЬКО фактическое разрешение (y in 0/1): null (сетевой сбой)
        # и -1 (ещё жив) должны перезапрашиваться в следующем прогоне, иначе одна
        # заминка сети навсегда выбрасывает рынок из сверки.
        if y in (0, 1):
            f.write(json.dumps({"asset": asset, "start": int(start), "code": code,
                                "y": y, "ptb": ptb, "fp": fp}) + "\n")
            f.flush()
        return y is not None

    with open(cache, "a") as f:
        if workers > 1:
            # Параллельная докачка (режим «прогнать платёж по всему датасету окон»).
            # Пул ограниченный: todo режется окном в 8*workers, чтобы обрыв по
            # «8 подряд пустых» не оставлял в очереди тысячи готовых к отправке
            # запросов — Cloudflare карает за это также, как за дубовый цикл.
            from concurrent.futures import ThreadPoolExecutor
            CH = 8 * workers
            with ThreadPoolExecutor(max_workers=workers) as ex:
                for i0 in range(0, len(todo), CH):
                    chunk = todo[i0:i0 + CH]
                    res = list(ex.map(lambda t: fetch_payment(t[0], t[1], t[2], timeout),
                                      chunk))
                    for (asset, start, code), (y, ptb, fp) in zip(chunk, res):
                        if not rec(asset, start, code, y, ptb, fp):
                            fails += 1
                    if fails >= 8:
                        print("  venue: 8 подряд пустых ответов — прекращаю (не долблю "
                              "IP: лимиты Cloudflare). Кэш целый, следующий прогон "
                              "продолжит с этого места.", file=sys.stderr)
                        break
                    print(f"  venue: {min(i0 + CH, len(todo))}/{len(todo)}",
                          file=sys.stderr)
            return out, len(todo)
        for n, (asset, start, code) in enumerate(todo):
            y, ptb, fp = fetch_payment(asset, start, code, timeout)
            if rec(asset, start, code, y, ptb, fp):
                fails = 0
                if n % 50 == 0:
                    print(f"  venue: {n + 1}/{len(todo)}", file=sys.stderr)
            else:
                fails += 1
                if fails >= 8:
                    print("  venue: 8 подряд пустых ответов — прекращаю (не долблю "
                          "IP: лимиты Cloudflare). Кэш целый, следующий прогон "
                          "продолжит с этого места.", file=sys.stderr)
                    break
            time.sleep(sleep)
    return out, len(todo)


def qmed(v, p=0.5):
    v = sorted(v)
    return round(v[int(p * (len(v) - 1))], 3) if v else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journal", default="/tmp/live-journal.jsonl")
    ap.add_argument("--cache", default="/tmp/kl")
    ap.add_argument("--log", default="", help="live.log — сверить число филлов")
    ap.add_argument("--days", type=int, default=12, help="сколько качать для --fetch-missing")
    ap.add_argument("--fetch-missing", action="store_true")
    ap.add_argument("--csv", default="")
    ap.add_argument("--bet-usd", type=float, default=50.0)
    ap.add_argument("--venue", action="store_true",
                    help="подтянуть РЕАЛЬНОЕ разрешение рынков из gamma-api "
                         "(slug из asset/code/start) и сравнить с нашим прокси-ярлыком")
    ap.add_argument("--venue-sample", type=int, default=200,
                    help="сколько рынков опросить (синглы всегда включаются)")
    ap.add_argument("--venue-cache", default="/tmp/venue.jsonl",
                    help="кэш ответов gamma (дополняется, перезапуск бесплатный)")
    ap.add_argument("--venue-keys", default="",
                    help="режим «платёж по списку ключей»: файл (jsonl или строки "
                         "«asset,start» / «asset,start,code») ИЛИ каталог/файл "
                         "windows_*.csv от clobwin — и тогда ключи берутся из "
                         "ВСЕХ окон суток, а не только из тех, где мы стояли. Нужен для "
                         "тейкер-исследования (takerprobe) и для H по всем окнам. Пишет в "
                         "тот же --venue-cache и выходит, ничего не считая")
    ap.add_argument("--venue-key-days", default="",
                    help="фильтр --venue-keys по суткам: 20260904-20260914,20260917-20260918")
    ap.add_argument("--venue-key-codes", default="5m", help="какие code брать из windows_*.csv")
    ap.add_argument("--venue-workers", type=int, default=1,
                    help="параллельных запросов к gamma (1 = как в ритуале; для тысяч "
                         "ключей ставь число ядер: 6)")
    ap.add_argument("--venue-sweep", type=int, default=0,
                    help="дополнительно опросить N ПОДРЯД идущих 5m-окон первого дня "
                         "журнала (безотносительно филлов) — нужно для H: там нужна "
                         "статистика по ВСЕМ окнам, а не только по тем, где мы стояли")
    ap.add_argument("--flip-margin", type=float, default=0.016,
                    help="доля изменения σ для теста маргинальных филлов")
    ap.add_argument("--start-after", default="",
                    help="считать ВСЕ блоки только по рынкам, чьё окно началось после "
                         "этого момента (epoch или «%m-%d %H:%M» UTC) — нужно, чтобы "
                         "отсечь часы с мёртвым спот-фидом: на них и решения, и метки")
    ap.add_argument("--start-before", default="", help="...и до этого момента")
    a = ap.parse_args()

    if a.venue_keys:
        import csv as _csv
        import glob as _glob
        want = []
        pk = a.venue_keys
        files = (sorted(_glob.glob(os.path.join(pk, "windows_*.csv")))
                 if os.path.isdir(pk) else [pk] if pk.endswith(".csv") else [])
        codes = set(x.strip() for x in a.venue_key_codes.split(",") if x.strip())
        daysel = set()
        if a.venue_key_days:
            import datetime as _dt
            for part in a.venue_key_days.split(","):
                if "-" in part:
                    x, y = part.split("-")
                    d0 = _dt.datetime.strptime(x, "%Y%m%d")
                    d1 = _dt.datetime.strptime(y, "%Y%m%d")
                    while d0 <= d1:
                        daysel.add(d0.strftime("%Y%m%d")); d0 += _dt.timedelta(days=1)
                else:
                    daysel.add(part)
        if files:
            for fp in files:
                with open(fp) as f:
                    for r in _csv.DictReader(f):
                        if (r.get("code") or "") not in codes:
                            continue
                        st = int(float(r["start"]))
                        if daysel and time.strftime("%Y%m%d", time.gmtime(st)) not in daysel:
                            continue
                        want.append((r["asset"], st, r["code"]))
        else:
            with open(pk) as f:
                for ln in f:
                    ln = ln.strip()
                    if not ln:
                        continue
                    if ln.startswith("{"):
                        r = json.loads(ln)
                        want.append((r["asset"], int(r["start"]), r.get("code", "5m")))
                    else:
                        pr = ln.replace(",", " ").split()
                        want.append((pr[0], int(pr[1]), pr[2] if len(pr) > 2 else "5m"))
        want = list(dict.fromkeys(want))
        if daysel:
            want = [w for w in want if time.strftime("%Y%m%d", time.gmtime(w[1])) in daysel]
        print(f"venue-keys: ключей {len(want)} (codes={','.join(sorted(codes))}, "
              f"сутки={'все' if not daysel else a.venue_key_days}), workers={a.venue_workers}",
              flush=True)
        vt, fresh = venue_truth(a.venue_cache, want, workers=a.venue_workers)
        okv = sum(1 for k in vt if vt[k]["y"] in (0, 1)
                  and vt[k].get("ptb") and vt[k].get("fp"))
        live = sum(1 for k in vt if vt[k]["y"] == -1)
        miss = len(want) - okv - live
        print(f"  ответов получено {fresh}, с платежом (ptb+fp+y) {okv} "
              f"({100.0 * okv / max(len(want), 1):.1f}%), ещё живых {live}, без данных {miss}")
        print(f"  кэш: {a.venue_cache} — он же читается --venue, так что A)/K)/H) "
              "перестанут зависеть от того, стояли ли мы в окне")
        if okv and not fresh:
            print("  новых запросов не было: весь список уже в кэше")
        return

    def cut(t):
        if not t:
            return None
        t = t.strip()
        if t.isdigit():
            return int(t)
        for fmt in ("%m-%d %H:%M", "%Y-%m-%d %H:%M", "%m-%d %H:%M:%S"):
            try:
                v = time.strptime(t, fmt)
                y = time.gmtime().tm_year
                return int(time.mktime((y, v.tm_mon, v.tm_mday, v.tm_hour,
                                        v.tm_min, v.tm_sec, 0, 0, -1))
                           - time.timezone)
            except Exception:
                pass
        raise SystemExit(f"не понял время {t!r}")

    lo, hi = cut(a.start_after), cut(a.start_before)
    ev, bad, nline = [], 0, 0
    with open(a.journal) as f:
        for ln in f:
            nline += 1
            try:
                r = json.loads(ln)
            except Exception:
                bad += 1
                continue
            ev.append(r)
    # целостность файла: journal пишется append-флажком и должен уметь только расти.
    # Битые строки = обрыв записи посередине файла (убитый процесс / ENOSPC).
    print(f"журнал: {os.path.getsize(a.journal)/1e6:.2f} МБ, строк {nline}, "
          f"разобралось {len(ev)}, мусора {bad}"
          + ("" if bad == 0 else "  ← битые строки: часть событий потеряна"))
    alln = len(ev)
    if lo or hi:
        ev = [e for e in ev if not e.get("start")
              or (lo or 0) <= int(e["start"]) <= (hi or 4e9)]
        print(f"срез по времени окон: {a.start_after or '—'} … {a.start_before or '—'} "
              f"(событий {alln} → {len(ev)}); вне среза блоки A)–K) не считают")
    fills = [e for e in ev if e.get("ev") == "fill"]
    intents = [e for e in ev if e.get("ev") == "intent"]
    mk_live = [e for e in ev if e.get("ev") == "markout" and e.get("src") == "resolve"]
    print(f"journal: событий {len(ev)}, fills {len(fills)}, intents {len(intents)}, "
          f"markout@resolve(уже живых) {len(mk_live)}")
    if a.log and os.path.exists(a.log):
        txt = open(a.log, errors="replace").read()
        tail = txt[-4000:]
        n = [int(x.split("=")[1]) for x in tail.split() if x.startswith("fills=")]
        if n:
            print(f"сверка с heartbeat: fills в логе {n[-1]} vs в journal {len(fills)} "
                  + ("OK" if n[-1] == len(fills) else
                     "(норма, если journal содержит несколько прогонов: fills в "
                     "heartbeat — счётчик ТЕКУЩЕГО процесса)"))

    # ---- собираем рынки: (asset,start) -> Legs, code, cp-и, цены ----
    mk = {}
    bench_rows = []   # (asset, start, cp, bench, spot, ts, bench_age) — что движок ДЕЛИЛ
    for e in intents:
        k = (e["asset"], e["start"])
        d = mk.setdefault(k, dict(code=e.get("code", "5m"), legs=[], cps=set()))
        d["cps"].add(e.get("cp"))
        if e.get("bench") is not None:
            d["bench"] = e["bench"]
            d["spot"] = e.get("spot")
            d["bench_age"] = max(d.get("bench_age") or 0, e.get("bench_age") or 0)
            bench_rows.append((e["asset"], int(e["start"]), e.get("cp"),
                               float(e["bench"]),
                               None if e.get("spot") is None else float(e["spot"]),
                               int(e.get("ts", 0)), e.get("bench_age")))
    for e in fills:
        d = mk.setdefault((e["asset"], e["start"]), dict(code="5m", legs=[], cps=set()))
        d["legs"].append(e)
    single = {k: v for k, v in mk.items() if len(v["legs"]) == 1}
    paired = sum(1 for v in mk.values() if len(v["legs"]) >= 2)
    nocode = [k for k, v in mk.items() if v["code"] not in WIN]
    print(f"рынков с филлами {len(mk)}, из них синглов {len(single)}, пар {paired}, "
          f"без известного code {len(nocode)}")

    data = {}
    for asset in set(k[0] for k in mk):
        sym = SYM.get(asset)
        if not sym:
            continue
        data[asset] = load_minutes(a.cache, sym, a.days, a.fetch_missing)
    cov = []
    for ass in sorted(data):
        mm = data[ass][0]
        if len(mm) < 2:
            cov.append(f"{ass}: ПУСТО")
            continue
        step = sorted(mm[i] - mm[i - 1] for i in range(1, len(mm)))
        tail = int(time.time() // 60 - mm[-1])
        cov.append(f"{ass}: {len(mm)} мин, медианный шаг {step[len(step) // 2]} мин, "
                   f"последняя {time.strftime('%m-%d %H:%M', time.gmtime(mm[-1] * 60))}"
                   f" (хвост {tail} мин)")
    print("кэш клинов: " + " | ".join(cov)
          + ("" if a.fetch_missing or not cov else
             "\n   кэш не докачивался: если «хвост» больше пары минут, метки A/B/F/J "
             "и блоки G/H/I по клинам считаются по неполным суткам — запусти с "
             "--fetch-missing"))

    rows_out, mo_live = [], []
    nocov = 0                          # рынки, которых нет в кэше клинов
    stale_eng = 0                      # движок решал по минуте возрастом >=2 мин
    dv, px_err, jrows = {}, [], []      # H)/G)/J) должны жить и без --venue
    ymap = {}                          # (asset,start) -> платёж площадки (fp>=ptb)
    mo_venue, vrows, dvmark = [], {}, {}   # то же для E)/K)
    gaps = []                     # (|fin−bench| в б.п., флип_60, флип_open) — near-tie
    agree_twin = agree_live = agree_open = amb = 0
    for (asset, start), v in sorted(single.items()):
        if (v.get("bench_age") or 0) >= 2:
            stale_eng += 1
            continue
        md = data.get(asset)
        if not md:
            continue
        mins, cl = md[0], md[1]
        end = start + WIN.get(v["code"], 300)
        fin = close_at(mins, cl, end - 1)
        bt = close_at(mins, cl, start)           # конвенция resolve()/clobmm — ЧИСТАЯ,
                                                 # не зависимая от того, какой билд
                                                 # стоял в процессе (иначе F/B/J/G
                                                 # стали бы несравнимы через границу
                                                 # деплоя)
        be = v.get("bench")                      # а это — чем ДЕЛИЛ x сам движок
        fl = close_at(mins, cl, start - 60)      # старая живая конвенция
        bo = est_at(md, end, "open_t")
        so = est_at(md, start, "open_t")
        y_o = None if bo is None or so is None else (1 if bo >= so else 0)
        # метка по ТОМУ, что платит площадка: TWAP60 на границе ≈ среднее o/h/l/c
        # минуты, заканчивающейся на границе (блок I: ошибка движения 0.91 б.п.
        # против 5.18 у close(start)) — заводим её полноправным ярлыком
        bm = est_at(md, start, "mean_p")
        fm = est_at(md, end, "mean_p")
        y_m = None if bm is None or fm is None else (1 if fm >= bm else 0)
        if fin is None or bt is None:
            # Кэш клинов не покрывает это окно (обрыв или дыра в середине).
            # Подставлять «последнюю доступную минуту» нельзя — именно это дало
            # вчерашние «офолдеры 100¢» и «ни та ни другая минута» в L: рынок
            # не измеряем, значит он не входит ни в один блок.
            nocov += 1
            continue
        if fl is None:
            fl = bt
        y_t = 1 if fin >= bt else 0
        y_l = 1 if fin >= fl else 0
        gaps.append((abs(fin - bt) / max(bt, 1e-9) * 1e4,
                     1 if y_t != y_l else 0,
                     0 if y_o is None else (1 if y_t != y_o else 0),
                     0 if y_m is None else (1 if y_t != y_m else 0),
                     0 if y_m is None else (1 if y_m != y_l else 0)))
        leg = v["legs"][0]
        px, lev = leg["price"], leg["level"]
        # journal px — УРОВЕНЬ YES-книги; наша нога для ask стоит 1−px (так же
        # делает движок: m.px = 1.0 − px_lvl, markout = (val − m.px)·100)
        pxe = px if lev == "bid" else 1.0 - px
        val = y_t if lev == "bid" else 1 - y_t
        d = (val - pxe) * 100.0
        mo_live.append(d)
        day = time.strftime("%Y-%m-%d", time.gmtime(start))
        rows_out.append(dict(day=day, asset=asset, start=start, code=v["code"],
                             lev=lev, px=round(pxe, 3), px_yes=round(px, 3),
                             y_twin=y_t, y_live=y_l, y_open=y_o, y_mean=y_m,
                             y_eng=None if be is None else (1 if fin >= be else 0),
                             bench_src=("engine" if v.get("bench") is not None else
                                        "kline"),
                             d_cents=round(d, 2), fin=fin, bt=bt, be=be))
        # (C) прокси «веры рынка» на эндшпиле
        # чекпоинты в live только c>=60 (register), поэтому прокси «веры рынка» —
        # mid книги в момент филла БЛИЗКО к концу окна, а не cp.
        t_close = end - int(leg.get("ts", end))
        m_mid = leg.get("mid")
        if t_close <= 20 and m_mid is not None and abs(m_mid - 0.5) >= 0.2:
            y_venue = 1 if m_mid > 0.5 else 0
            if y_venue == y_t:
                agree_twin += 1
            if y_venue == y_l:
                agree_live += 1
            if y_venue == y_o:
                agree_open += 1
        elif t_close <= 20 and m_mid is not None:
            amb += 1

    print("\nA) ГЕЙТ M9 №2, восстановленный по journal (markout синглов, ¢):")
    if mo_live:
        d = sorted(mo_live)
        n = len(d)
        neng = sum(1 for r in rows_out if r["bench_src"] == "engine")
        if stale_eng:
            print(f"   ОТБРОШЕНО {stale_eng} синглов, где движок решил по минуте "
                  "возрастом ≥2 мин (bench_age в его же intent): это не исполнение "
                  "нашей стратегии, а исполнение по замороженной/пропущенной минуте, "
                  "и в гейт №2/№4 такие филлы не входят")
        if nocov:
            print(f"   вне покрытия клинов {nocov} синглов: они исключены ВСЕМИ "
                  "блоками, а не размечены «последней доступной минутой» (сверь "
                  "«синглов» в шапке с n в таблицах)")
        print(f"   телеметрия движка (intent.bench) есть для {neng}/{len(rows_out)} "
              "рынков; метки в A)/B)/F)/J) считаются ПО КЛИНАМ (close(start)), чтобы "
              "таблицы оставались сравнимыми через границу билда — иначе день до "
              "рестарта и после давали бы разные ярлыки под одним и тем же названием")
        print(f"   n={n}  медиана={d[n // 2]:.2f}¢ (гейт ≥ −0.5¢)  "
              f"доля < −1¢ = {100 * sum(1 for x in d if x < -1) / n:.0f}%  "
              f"среднее={sum(d) / n:.2f}¢")
        byday = {}
        for r in rows_out:
            byday.setdefault(r["day"], []).append(r["d_cents"])
        for day in sorted(byday):
            v = sorted(byday[day])
            print(f"   {day}: n={len(v):>4} медиана={v[len(v) // 2]:>6.2f}¢ "
                  f"p10={qmed(v, .1):>7.2f} p90={qmed(v, .9):>7.2f}")
    else:
        print("   синглов с филлами нет — проверь, что journal не пустой")

    if mk_live:
        key_live = {(e["asset"], e["start"]): e for e in mk_live}
        diffs = []
        cold = stale = quant = 0
        off = []
        # Сегменты журнала = «жизни одного процесса»: разрыв > 900 с = рестарт.
        # MinuteBook живёт в процессе, поэтому у рынка нет минуты start только
        # тогда, когда процесс стартовал уже внутри (или после) его окна.
        tss = sorted({int(e["ts"]) for e in ev if e.get("ts")})
        starts = [tss[0]] if tss else []
        for a1, b1 in zip(tss, tss[1:]):
            if b1 - a1 > 900:
                starts.append(b1)
        def proc_start(t):
            k = 0
            for i, x in enumerate(starts):
                if x <= t:
                    k = i
            return starts[k]
        hit = {t: 0 for t in ("y_twin", "y_live", "y_open", "y_mean", "y_eng")}
        for r in rows_out:
            m = key_live.get((r["asset"], r["start"]))
            if m is None:
                continue
            # САМОПРОВЕРКА = «наша реконструкция совпадает с движком», а НЕ «движок
            # использует удобную нам конвенцию»: сравнивать надо с разметкой ПО ТОЙ
            # МИНУТЕ, которую движок сам записал (intent.bench). Иначе старый билд
            # честно выдавал бы 100¢ «расхождения» там, где расхождения нет.
            ye = r.get("y_eng")
            dref = r["d_cents"] if ye is None else \
                ((ye if r["lev"] == "bid" else 1 - ye) - r["px"]) * 100.0
            dd = abs(m["d_cents"] - dref)
            # bench/spot в journal пишутся с округлением до 2 знаков после точки:
            # для BTC это 0.05 б.п., для XRP по цене $2.8 — 18 б.п. Значит у дешёвых
            # активов наш y_eng (fin >= bench_из_журнала) физически не воспроизводит
            # решение движка, когда |fin−bench| мельче кванта: это не расхождение
            # формул, а отсутствие разрешения. Считаем такие рынки отдельно.
            if dd >= 0.08 and r.get("be"):
                q_bps = 0.005 / r["be"] * 1e4
                if abs(r["fin"] - r["be"]) / r["be"] * 1e4 < 2 * q_bps:
                    quant += 1
                    continue
            # «холодный старт»: мы увидели рынок ПОСЛЕ начала его окна ⇒ у MinuteBook
            # не было минуты start, и resolve() метил по последней доступной минуте.
            # Это не разнобой формул, и в «max расхождение» такое считать нельзя.
            ml = key_live.get((r["asset"], r["start"])) or {}
            # Причина №1 потери минуты — не только холодный старт процесса, но и
            # дыра в спот-фиде: MinuteBook.close() отдаёт ПОСЛЕДНЮЮ минуту <= t,
            # т.е. при замёрзшем фиде bench/fin берутся часы назад. Новый билд пишет
            # bench_age (сколько минут не хватило) — по нему рынок отсекается точно.
            ag = ml.get("bench_age")
            if dd >= 0.05 and (ag is not None and ag >= 2):
                stale += 1
                continue
            if dd >= 0.08 and proc_start(r["start"] + 300) >= r["start"] + 60:
                cold += 1
                continue
            if dd >= 0.08:
                off.append((dd, r["asset"], r["start"], ml, r, dref))
            diffs.append(dd)
            for t in hit:
                y = r[t]
                if y is None:
                    continue
                vv = y if r["lev"] == "bid" else 1 - y
                # px и d_cents в journal округлены до 3/2 знаков ⇒ допуск 0.08¢,
                # иначе сравнение «с кем совпало» врёт на половинках шага квантования
                if abs((vv - r["px"]) * 100 - m["d_cents"]) < 0.08:
                    hit[t] += 1
        print(f"   чью конвенцию размечает движок (по его же markout@resolve, n={len(diffs)}): "
              f"close(start) = {hit['y_twin']}, close(start−60) = {hit['y_live']}, "
              f"свеча open(start) = {hit['y_open']}, mean_p=TWAP площадки "
              f"= {hit['y_mean']}")
        if cold or stale or quant:
            print(f"   отброшено как НЕПРОВЕРЯЕМОЕ: квантование телеметрии "
                  f"мельче шага метки {quant}, холодный старт процесса "
                  f"{cold}, дыра/заморозка спот-фида в момент метки (bench_age ≥ 2) "
                  f"{stale} — MinuteBook отдаёт последнюю минуту <= t, поэтому и "
                  "решение, и метка могут считаться по цене часовой давности "
                  "(расхождение у таких ровно 100¢, в статистику не входит)")
        for dd, ass, st, ml, r, dref in sorted(off, reverse=True)[:6]:
            print(f"   офолдер: {ass} {time.strftime('%m-%d %H:%M', time.gmtime(st))} "
                  f"движок={ml.get('d_cents')}¢ реконструкция={dref:.2f}¢ "
                  f"разница {dd:.2f}¢ px={r['px']} lev={r['lev']} "
                  f"bench_age={ml.get('bench_age')}")
        print(f"   сегментов процесса в журнале: {len(starts)}"
              + ("" if not starts else
                 " (" + ", ".join(time.strftime("%m-%d %H:%M", time.gmtime(x))
                                  for x in starts[:8]) + ("…)" if len(starts) > 8 else ")")))
        if diffs:
            print(f"   САМОПРОВЕРКА (движок против ЕГО СОБСТВЕННОЙ минуты): {len(diffs)} рынков, где движок сам записал "
                  f"markout@resolve; max расхождение с реконструкцией = {max(diffs):.2f}¢ "
                  + ("( OK — end/bench/кэш совпадают с движком )" if max(diffs) < 0.05
                     else "( РАСХОЖДЕНИЕ — реконструкцию A не читать, искать разницу в минутах )"))

    print("\nB) Цена вопроса про конвенцию bench (start vs start−60):")
    if rows_out:
        n = len(rows_out)
        have_op = any(r.get("y_mean") is not None for r in rows_out)
        for tag, gi in (("resolve()/clobmm close(start) ↔ live close(start−60)", 1),
                        ("resolve()/clobmm close(start) ↔ свеча open(start)", 2),
                        ("close(start) ↔ mean_p (TWAP площадки)", 3),
                        ("close(start−60) ↔ mean_p (TWAP площадки)", 4)):
            if gi in (2, 3, 4) and not have_op:
                print(f"   {tag}: нет open/hl в кэше — пересними клины: "
                      f"python3 {sys.argv[0]} --cache /tmp/kl5 --fetch-missing --days "
                      f"{a.days} и повтори с --cache /tmp/kl5")
                continue
            fl = sum(g[gi] for g in gaps)
            hard = [g for g in gaps if g[0] >= 1.0]
            flh = sum(g[gi] for g in hard)
            print(f"   флип {tag}: {fl}/{n} = {100 * fl / n:.2f}%"
                  + (f"; вне near-tie (|fin−bench|≥1 б.п.) {flh}/{len(hard)} = "
                     f"{100 * flh / len(hard):.2f}%" if hard else ""))
        print(f"   exact-tie: |fin−bench|<0.05 б.п. {sum(1 for g in gaps if g[0] < 0.05)}/{n}, "
              f"<1 б.п. {sum(1 for g in gaps if g[0] < 1.0)}/{n}; "
              f"медиана |fin−bench| = {qmed([g[0] for g in gaps]):.2f} б.п.")
        print("   если флипы ~0% — расхождение live↔clobmm в знаменателе x не влияет "
              "на P&L/markout и гейты; если >1% — это дефект формулы, он первее всех гейтов.")
    print("\nC) Кто ближе к «вере рынка» на эндшпиле (филл в ≤20 с до конца окна, |mid−0.5|≥0.2):")
    print(f"   совпадений: resolve()/clobmm close(start) = {agree_twin}, "
          f"live-checkpoint close(start−60) = {agree_live}, свеча open(start) = "
          f"{agree_open}, отброшено near-tie = {amb}")
    if agree_twin + agree_live == 0:
        print("   покрытие нулевое: на накопленных сутках этот спор не решается — "
              "нужны эндшпильные филлы с |mid−0.5|≥0.2 или рынок-мид из clob-лога "
              "на расчётной машине (21.09).")

    # ---- F) калибровка: то, что РЕШАЕТ, а не медиана markout ----
    def ev_table(rows, ymap, label, yfield="y_twin"):
        print(f"   {label}")
        print(f"   {'цена входа':>12} {'n':>5} {'win%':>6} {'имплиц.':>8} {'edge':>7} "
              f"{'EV¢/100акц':>11}")
        for lo, hi in ((0.0, .10), (.10, .30), (.30, .70), (.70, .90), (.90, 1.0001)):
            sel = [r for r in rows if lo <= r["px"] < hi]
            ys = []
            for r in sel:
                y = (ymap.get((r["asset"], r["start"])) if ymap else None)
                if y is None:
                    y = r[yfield]
                if y in (0, 1):
                    ys.append((y, r["px"], r["lev"]))
            if not ys:
                continue
            # val = y для YES-ноги, 1−y для NO-ноги (валюация движка)
            vals = [(y if lev == "bid" else 1 - y) for y, _, lev in ys]
            win = sum(vals) / len(vals)
            pxm = sum(p for _, p, _ in ys) / len(ys)
            ev = sum((v - px) for v, (_, px, _) in zip(vals, ys)) / len(ys) * 100.0
            print(f"   {lo:>5.2f}-{hi:>5.2f} {len(ys):>5} {100 * win:>5.1f}% "
                  f"{100 * pxm:>7.1f}% {100 * (win - pxm):>+6.1f}п.п. {ev:>+11.2f}")
        return

    print("\nF) КАЛИБРОВКА синглов (решает M9, а не медиана markout).")
    print("   «цена входа» = цена НАШЕЙ НОГИ (для ask-исполнения это 1−YES-уровень, "
          "ровно как m.px/val−px в движке); win = валюация резолва.")
    if rows_out:
        ev_table(rows_out, None, "ярлык = resolve()/clobmm (fin ≥ close(start)):")
        ev_table(rows_out, None, "ярлык = live-решение (fin ≥ close(start−60)):",
                 "y_live")
        if any(r["y_open"] is not None for r in rows_out):
            ev_table(rows_out, None,
                     "ярлык = свеча на границах (open(end) ≥ open(start)):", "y_open")
        if any(r.get("y_mean") is not None for r in rows_out):
            ev_table(rows_out, None,
                     "ярлык = mean_p: среднее минуты ДО границы, т.е. TWAP площадки "
                     "(ИМЕННО ПО НЕМУ ПЛАТЯТ):", "y_mean")
        print("   чтение: win% ≈ цены входа (|edge| мал) ⇒ рынка мы ни переоцениваем, "
              "ни недооцениваем: нет ни edge, ни катастрофы. Медиана markout при этом "
              "ОТРИЦАТЕЛЬНА механически (дешёвый вход проигрывает часто, но теряет "
              "только свою цену), поэтому гейт №2 надо читать вместе с этой таблицей, "
              "а не вместо неё. Edge < −3 п.п. = нас реально съедают.")
    else:
        print("   нет синглов с восстановленными исходами")

    vt = {}
    vt = {}
    if a.venue:
        singles = sorted(single.keys())
        others = [k for k in sorted(mk.keys()) if k not in single]
        pick = [(k[0], k[1], mk[k]["code"]) for k in (singles + others)[:a.venue_sample]]
        if a.venue_sweep > 0:
            # детерминированные slug'ы: каждые 300 с от полуночи первого дня журнала
            t0 = min([s0 for (_a, s0) in mk] or [int(time.time())]) // 86400 * 86400
            have = {k[:2] for k in pick}
            for asset in ("btc", "eth"):
                for k in range(a.venue_sweep):
                    st = t0 + 300 * k
                    if (asset, st) not in have:
                        pick.append((asset, st, "5m"))
        print(f"   (кэш {a.venue_cache}: {len(pick)} ключей на сверку)", file=sys.stderr)
        print(f"\nE) ПРАВДА ПЛОЩАДКИ (gamma, {len(pick)} рынков): сверка нашего прокси-ярлыка")
        vt, fresh = venue_truth(a.venue_cache, pick, workers=a.venue_workers)
        dis_t = dis_l = dis_o = dis_m = res = res_o = res_m = openn = 0
        mo_venue = []
        px_err = []        # (б.п.: bench close(start)−ptb, bench close(start−60)−ptb, fin−fp)
        jrows = []         # {t,l,o,v} -> markout ¢/100акц на ОДНОМ и том же наборе
        vrows = []         # те же рынки, но ярлык = платёж площадки (для K)
        dvmark = {}        # (asset,start) -> markout ¢ по платежу
        dv = {}            # (asset,start) -> (Δ_площадка б.п., Δ_наш_прокси б.п.)
        exact = {0: 0, 1: 0}
        pl_proxy, pl_venue = [], []
        ymap = {}
        for (asset, start), v in mk.items():
            rec = vt.get((asset, start)) or {}
            y_venue = rec.get("y")
            if y_venue is None or y_venue < 0:
                if y_venue == -1:
                    openn += 1
                continue
            if rec.get("ptb") is not None and rec.get("fp") is not None:
                ymap[(asset, start)] = 1 if rec["fp"] >= rec["ptb"] else 0
                exact[1 if ymap[(asset, start)] == y_venue else 0] += 1
            md = data.get(asset)
            if not md or len(v["legs"]) != 1:
                continue
            mins, cl = md[0], md[1]
            end = start + WIN.get(v["code"], 300)
            fin = close_at(mins, cl, end - 1)
            bt = close_at(mins, cl, start)
            fl = close_at(mins, cl, start - 60) or bt
            if fin is None or bt is None:
                continue
            if rec.get("ptb") and rec.get("fp"):
                px_err.append((round((bt / rec["ptb"] - 1) * 1e4, 2),
                               round((fl / rec["ptb"] - 1) * 1e4, 2),
                               round((fin / rec["fp"] - 1) * 1e4, 2)))
            res += 1
            yt = 1 if fin >= bt else 0
            yl = 1 if fin >= fl else 0
            bo, so = est_at(md, end, "open_t"), est_at(md, start, "open_t")
            yo = None if bo is None or so is None else (1 if bo >= so else 0)
            bm, fm = est_at(md, start, "mean_p"), est_at(md, end, "mean_p")
            ym = None if bm is None or fm is None else (1 if fm >= bm else 0)
            dis_m += (ym is not None and ym != y_venue)
            res_m += (ym is not None)
            dis_t += (yt != y_venue)
            dis_l += (yl != y_venue)
            dis_o += (yo is not None and yo != y_venue)
            res_o += (yo is not None)
            leg = v["legs"][0]
            lev, px = leg["level"], leg["price"]
            pxe = px if lev == "bid" else 1.0 - px
            val = y_venue if lev == "bid" else 1 - y_venue
            mo_venue.append((val - pxe) * 100.0)
            dvmark[(asset, start)] = (val - pxe) * 100.0
            vp = yt if lev == "bid" else 1 - yt
            pl_proxy.append((vp - pxe) * 100.0)
            pl_venue.append((val - pxe) * 100.0)
            jrow = {"t": (vp - pxe) * 100.0, "v": (val - pxe) * 100.0}
            vl = yl if lev == "bid" else 1 - yl
            jrow["l"] = (vl - pxe) * 100.0
            if yo is not None:
                vo = yo if lev == "bid" else 1 - yo
                jrow["o"] = (vo - pxe) * 100.0
            if ym is not None:
                vm = ym if lev == "bid" else 1 - ym
                jrow["m"] = (vm - pxe) * 100.0
            jrows.append(jrow)
            vrows.append(dict(day=time.strftime("%Y-%m-%d", time.gmtime(start)),
                              asset=asset, start=start, code=v["code"], lev=lev,
                              px=round(pxe, 3), y_twin=y_venue, d_cents=None))
        print(f"   свежих запросов {fresh}, сопоставлено закрытых {res}, ещё живых {openn}")
        if rows_out and ymap:
            ev_table([r for r in rows_out if (r["asset"], r["start"]) in ymap], ymap,
                     "ярлык = ФАКТИЧЕСКОЕ разрешение площадки:")
        if res:
            print(f"   РАСХОДИТСЯ с фактическим разрешением площадки: "
                  f"resolve()/clobmm close(start) = {dis_t}/{res} = "
                  f"{100 * dis_t / res:.1f}%;  live close(start−60) = {dis_l}/{res} = "
                  f"{100 * dis_l / res:.1f}%"
                  + (f";  свеча open(start) = {dis_o}/{res_o} = "
                     f"{100 * dis_o / res_o:.1f}%" if res_o else "")
                  + (f";  mean_p (TWAP) = {dis_m}/{res_m} = "
                     f"{100 * dis_m / res_m:.1f}%" if res_m else ""))
            print("   вот это число решает всё: если >1-2%, то P&L и markout в clobmm/M5/M6 "
                  "считаны по ярлыку, который площадка не платит, — вердикты пересаживаются "
                  "на venue-ярлык, и только потом обсуждаются торговые правки.")
        if mo_venue:
            d = sorted(mo_venue)
            n = len(d)
            print(f"   A') медиана markout по ФАКТИЧЕСКОМУ разрешению: {d[n // 2]:.2f}¢ "
                  f"(n={n}, доля < −1¢ = {100 * sum(1 for x in d if x < -1) / n:.0f}%)")
        else:
            print("   покрытие нулевое: синглы не сопоставились (кэш клинов/asset вне btc-eth?)")

        print("\nG) НАСКОЛЬКО НАШ ПРОКСИ — НЕ ТОТ РЫНОК (в б.п., по точным ценам площадки):")
        print("   правило площадки: Up ⇔ TWAP60(Chainlink BTC/USD) на конце окна ≥ TWAP на"
              " начале; ties ⇒ Up. Мы же размечаем по Binance 1m close.")
        print(f"   сверка ярлыка: outcomePrice ↔ (finalPrice ≥ priceToBeat) совпало "
              f"{exact[1]}/{exact[0] + exact[1]} (иначе у меня ошибка в разборе payload)")
        if not px_err:
            print("   нет ptb/fp: либо --venue не передан, либо кэш старый (в нём только "
                  "y) — тогда rm -f " + a.venue_cache + " и перезапрос")
        else:
            for tag, i in (("bench = close(start)   [resolve()/clobmm]", 0),
                           ("bench = close(start−60) [live checkpoint()]", 1),
                           ("fin   = close(end−1)   против finalPrice", 2)):
                v = sorted(abs(x[i]) for x in px_err)
                sg = sorted(px_err, key=lambda x: abs(x[i]))[len(px_err) // 2][i]
                print(f"     {tag}: медиана |ошибки| = {qmed(v):>7.2f} б.п.   "
                      f"p90 = {v[int(0.9 * (len(v) - 1))]:>7.2f}   "
                      f"медиана со знаком = {sg:>+7.2f}")
            if gaps:
                print("     (для масштаба: медиана |fin−bench| по нашему прокси = "
                      f"{qmed([g[0] for g in gaps]):.2f} б.п. — если ошибка прокси того же "
                      "порядка, метка швыряется монеткой)")
            if pl_proxy and pl_venue:
                n = len(pl_proxy)
                bp = a.bet_usd / 0.5 / 100.0     # акций на рынок при ставке bet_usd
                dif = [b - x for x, b in zip(pl_proxy, pl_venue)]
                na = sum(1 for x in dif if abs(x) > 1)
                print(f"   П&Л тех же {n} синглов, ¢ на рынок: прокси = {sum(pl_proxy):+.2f} "
                      f"→ площадка = {sum(pl_venue):+.2f}  "
                      f"(разница {sum(dif):+.2f}¢, "
                      f"≈ {sum(dif) / 100.0 * bp:+.2f}$ при {a.bet_usd:.0f}$ на входе)")
                print(f"   НО: метки разошлись на {na}/{n} = {100 * na / n:.1f}% рынка, а "
                      f"сумма |разниц| = {sum(abs(x) for x in dif):.2f}¢ при net "
                      f"{sum(dif):+.2f}¢ ⇒ ошибка метки НЕ смещает P&Л (смещение уровня "
                      "Binance↔Chainlink сокращается в разности границ), а РАЗДУВАЕТ его "
                      f"дисперсию: ±{sum(abs(x) for x in dif) / max(1, n):.1f}¢/100акц "
                      "лотереи на рынок.")
                print("   это цена того, что M5/M6 меряли на прокси: если |разница| "
                      "сопоставима с самим P&L, вердикт M5 надо переснять на точных "
                      "ценах площадки, а не обсуждать правки модели.")

    for (asset, start), rec in sorted(vt.items()):
        if not rec or rec.get("ptb") is None or rec.get("fp") is None:
            continue
        md = data.get(asset)
        if not md:
            continue
        mins, cl = md[0], md[1]
        end = start + WIN.get(rec.get("code") or "5m", 300)
        bt, fin = close_at(mins, cl, start), close_at(mins, cl, end - 1)
        if not bt or not fin:
            continue
        dv[(asset, start)] = (round((rec["fp"] / rec["ptb"] - 1) * 1e4, 2),
                              round((fin / bt - 1) * 1e4, 2))
    print("\nH) НАША σ̂ МЕРЯЕТ НЕ ВЕЛИЧИНУ ПЛОЩАДКИ (границы окна по числам площадки):")
    if not a.venue:
        print("   пропущено: нужен --venue (ptb/fp из gamma). H)/I)/G)/J) без него не "
              "считаются — это те блоки, которые решают спор о минуте.")
    if dv:
        av = sorted(abs(x[0]) for x in dv.values())
        ap = sorted(abs(x[1]) for x in dv.values())
        rv = sum(av) / len(av)
        rp = sum(ap) / len(ap)
        ratio = rv / rp if rp > 0 else float("nan")
        print(f"   n={len(dv)}  среднее |Δ| за окно: площадка (TWAP) = {rv:.2f} б.п.,  "
              f"наш прокси (spot-кеи close) = {rp:.2f} б.п.  →  сглаживание ×{ratio:.3f}")
        # σ̂ тем же методом, что MinuteBook: std log-возвратов 1м за 120 мин ×√5,
        # НО на старте каждого окна — ровно та σ̂, с которой движок решал. Раньше
        # здесь стояли «последние 120 минут кэша», из-за чего одна докачка кэша на
        # 4.7 часа сдвинула цифру с 13.99 на 39.40 б.п., не изменив ни одного
        # решения: это была волатильность «сейчас», а не волатильность прогона.
        sgm = []
        for (asset, start) in dv:
            md = data.get(asset)
            if not md:
                continue
            mins, cl = md[0], md[1]
            i1 = bisect.bisect_right(mins, int(start) // 60) - 1
            if i1 < 121:
                continue
            r = [math.log(cl[i] / cl[i - 1]) * 1e4
                 for i in range(i1 - 119, i1 + 1)
                 if cl[i - 1] > 0 and mins[i] - mins[i - 1] <= 2]
            if len(r) > 100:
                sgm.append((sum(x * x for x in r) / len(r)) ** .5 * 5 ** .5)
        if sgm:
            sgm.sort()
            sh = sum(sgm) / len(sgm)
            print(f"   σ̂_spot (как в MinuteBook, 120 мин ×√5, на старте окна; "
                  f"n={len(sgm)} из {len(dv)}): медиана {sgm[len(sgm) // 2]:.2f}, "
                  f"среднее {sh:.2f} б.п./5м  →  "
                  f"σ_площадки ≈ {sh * ratio:.2f} б.п.  (×{ratio:.3f})")
            wider = ratio > 1
            print(f"   следствия по формуле: x = d/(σ̂√(τ/300)) ⇒ при верном масштабе "
                  f"|x| уменьшился бы в {ratio:.2f}×, а h = k·pdf(x)·√(step/τ) стал бы "
                  + (f"ШИРЕ в ~{ratio:.2f}× — сейчас мы держим СПРЕД УЖЕ, чем платит "
                     "рынок, и нас снимают по цене, которая нам невыгодна" if wider else
                     f"УЖЕ в ~{1 / ratio:.2f}× — сейчас мы держим спред шире, чем платит рынок"))
            print("   причина НЕ в «TWAP сглаживает»: close_t берёт бенч = close минуты, "
                  "содержащей start, т.е. первая минута окна съедена самим бенчем — мы "
                  "мерим ~4-минутное движение, а платят за 5-минутное. Отсюда |Δ| "
                  "площадки БОЛЬШЕ нашего, и σ̂ надо не уточнять, а масштабировать.")
            print("   ВНИМАНИЕ: это НЕ правка модели и НЕ «подстройка σ под результат» — это "
                  "размерная единица величины, по которой платят. Применять только к обоим "
                  "файлам-близнецам сразу и только после того, как n ≥ 300 окон и ratio "
                  "устойчив по дням; до этого — ничего не трогаем.")
    else:
        print("   нет точных Δ (нужен --venue с priceToBeat/finalPrice в кэше)")

    KINDS = ("close_t", "close_p", "mean_t", "mean_p", "open_t")
    LN = {"close_t": "close минуты, содержащей границу (ныне: clobmm/resolve)",
          "close_p": "close минуты ДО границы       (ныне: живое checkpoint)",
          "mean_t": "среднее o/h/l/c минуты, содержащей границу",
          "mean_p": "среднее o/h/l/c минуты ДО границы  ← прокси TWAP60 площадки",
          "open_t": "open ровно на границе"}
    er = {k: [] for k in KINDS}
    if not a.venue:
        KINDS = ()          # без ptb/fp сравнивать не с чем
    for (asset, start), rec in sorted(vt.items()):
        if not rec or rec.get("ptb") is None or rec.get("fp") is None:
            continue
        md = data.get(asset)
        if not md:
            continue
        end = start + WIN.get(rec.get("code") or "5m", 300)
        vals = {}
        for k in KINDS:
            b, f = est_at(md, start, k), est_at(md, end, k)
            if b is None or f is None or b <= 0 or f <= 0:
                vals = {}
                break
            vals[k] = ((b / rec["ptb"] - 1) * 1e4, (f / rec["fp"] - 1) * 1e4,
                       (f / b - 1) * 1e4 - (rec["fp"] / rec["ptb"] - 1) * 1e4,
                       (f / b - 1) * 1e4)
        for k, v in vals.items():
            er[k].append(v)
    print("\nJ) АРБИТР КОНВЕНЦИЙ ПО ТОЧНЫМ ЦЕНАМ ПЛОЩАДКИ (один и тот же набор рынков):")
    if jrows:
        NM = {"t": "close(start)    = метка сима (clobmm) и live resolve()",
              "l": "close(start−60) = знаменатель x в live checkpoint()",
              "o": "open(start)     = цена ровно на границе окна",
              "m": "mean_p          = среднее минуты ДО границы = TWAP площадки"}
        nv = [r["v"] for r in jrows]
        print(f"   n={len(jrows)} синглов, у которых есть ptb/fp; «ошибка» = markout по "
              "этой метке минус markout по платёжу площадки (¢/100 акций)")
        print(f"   ПЛОЩАДКА:            медиана {qmed(nv):>7.2f}¢  среднее "
              f"{sum(nv) / len(nv):>+7.2f}¢  Σ {sum(nv):>+9.2f}¢")
        for k in ("t", "l", "o", "m"):
            col = [r[k] for r in jrows if k in r]
            if len(col) < 5:
                continue
            dif = [r[k] - r["v"] for r in jrows if k in r]
            n2 = len(dif)
            flips = sum(1 for x in dif if abs(x) > 1)
            se = (0.25 / n2) ** .5 * 100
            print(f"   {NM[k]}: медиана {qmed(col):>7.2f}¢  среднее "
                  f"{sum(col) / n2:>+7.2f}¢  Σ {sum(col):>+9.2f}¢ | ошибка: смещение "
                  f"{sum(dif) / n2:>+6.2f}¢ ± "
                  f"{(sum((x - sum(dif) / n2) ** 2 for x in dif) / n2) ** .5 / n2 ** .5:>4.2f}"
                  f"  размах {sum(abs(x) for x in dif) / n2:>5.2f}¢  метку не угадали "
                  f"{flips}/{n2} = {100 * flips / n2:.1f}% ± {se:.1f}")
        print("   как читать: смещение ≈ 0 у всех ⇒ метки несмещены и выбор меняет только "
              "дисперсию (её и надо минимизировать); смещение, превышающее своё ± ⇒ эта метка врёт про P&Л, и близнеца надо вести к цифре площадки, а не к "
              "другой минуте Binance.")
    else:
        print("   нет сопоставленных синглов с ptb/fp — прогони с --venue (кэш venue уже "
              "наполнен, запросов будет мало)")

    print("\nI) КАКАЯ ОЦЕНКА ГРАНИЦЫ ЕСТЬ TWAP ПЛОЩАДКИ (по ptb/fp из gamma):")
    if not a.venue:
        print("   пропущено: нужен --venue")
    print("   сравниваем не УРОВЕНЬ (Binance↔Chainlink смещён на константу, она "
          "сокращается в разности границ), а ОШИБКУ ДВИЖЕНИЯ fin/bench − (fp/ptb): "
          "именно она решает метку")
    best = None
    av_venue = (sum(abs(x[0]) for x in dv.values()) / len(dv)) if dv else float("nan")
    for k in KINDS:
        rows = er[k]
        if len(rows) < 8:
            nohl = any(md[3] is None for md in data.values())
            print(f"   {LN[k]}: n={len(rows)}"
                  + (" — нет o/h/l в кэше: --cache /tmp/kl5 --fetch-missing"
                     if nohl and k in ("mean_t", "mean_p", "open_t")
                     else " — мало рынков (нужно ≥ 8)")
                  if len(rows) < 8 else "")
            continue
        mv = sum(r[2] for r in rows) / len(rows)
        sd = (sum((r[2] - mv) ** 2 for r in rows) / len(rows)) ** .5
        med = qmed([abs(r[2]) for r in rows])
        print(f"   {LN[k]}: n={len(rows):>4}  смещение бенча "
              f"{qmed([r[0] for r in rows]):>+7.2f}  |Δбенч| {qmed([abs(r[0]) for r in rows]):>6.2f}"
              f"  |Δфинал| {qmed([abs(r[1]) for r in rows]):>6.2f}   "
              f"ОШИБКА ДВИЖЕНИЯ медиана |·| = {med:>6.2f} б.п., σ = {sd:>6.2f}   "
              f"|Δ| этой оценки = {sum(abs(r[3]) for r in rows) / len(rows):>5.2f} б.п."
              f" (площадка {av_venue:.2f}, ×{(sum(abs(r[3]) for r in rows) / len(rows)) / av_venue if av_venue == av_venue and av_venue else float('nan'):.3f})"
)
        if best is None or med < best[1]:
            best = (k, med, len(rows))
    if best and len(er["close_t"]) >= 8:
        cm = qmed([abs(r[2]) for r in er["close_t"]])
        cp = (qmed([abs(r[2]) for r in er["close_p"]])
              if len(er["close_p"]) >= 8 else float("nan"))
        print(f"   ⇒ наименьшая ошибка движения у «{best[0]}» ({best[1]:.2f} б.п.) при "
              f"n={best[2]}; у нынешней разметки сима «close_t» — {cm:.2f} б.п. "
              f"(живой знаменатель x, «close_p» — {cp:.2f}); на фоне "
              f"медианы |fin−bench| по прокси "
              f"{qmed([g[0] for g in gaps]) if gaps else float('nan'):.2f} б.п. — это и есть "
              "причина ~20% флипов метки.")
        if best[0] != "close_t":
            print("   вердикт: менять оценку границы надо ОДНОВРЕМЕННО в clobmm и в "
                  "KronoTrade (и метку, и знаменатель x) — это не правка модели, а выбор "
                  "измеряемой величины; гейт №2 и паритет P&L после этого переснимаются.")

    print("\nK) ГЕЙТ M9 №2 НА ДЕНЬГАХ ПЛОЩАДКИ (ярлык = finalPrice ≥ priceToBeat):")
    if vrows:
        d = sorted(mo_venue)
        n = len(d)
        print(f"   n={n}  медиана={d[n // 2]:.2f}¢ (порог ≥ −0.5¢)  среднее="
              f"{sum(d) / n:.2f}¢  доля < −1¢ = {100 * sum(1 for x in d if x < -1) / n:.0f}%")
        byday = {}
        for (asset, start), rec in sorted(dvmark.items()):
            byday.setdefault(time.strftime("%Y-%m-%d", time.gmtime(start)), []).append(rec)
        for day in sorted(byday):
            v = sorted(byday[day])
            print(f"   {day}: n={len(v):>4} медиана={v[len(v) // 2]:>7.2f}¢  "
                  f"среднее={sum(v) / len(v):>7.2f}¢  p10={qmed(v, .1):>7.2f} "
                  f"p90={qmed(v, .9):>7.2f}")
        print("   вот это и есть число, которое надо сравнить с −0.5¢: без нашего "
              "прокси-шума, без выбора минуты, на том, что реально платит рынок. "
              "Медиана по-прежнему механически отрицательна для дешёвых входов — "
              "рядом с ней калибровка:")
        ev_table(vrows, None, "ярлык = ПЛАТЁЖ ПЛОЩАДКИ:")
    else:
        print("   нет рынков с ptb/fp (нужен --venue)")

    print("\nL) ДЕПЛОЙ: чем движок реально делил x (intent.bench ↔ минуты клинов).")
    print("   Уровень live-фида и клинов сходится с точностью до базиса (разные "
          "инструменты/витрины), поэтому метрика нормирована на базис суток — "
          "осталось только «попадание в минуту», которое нас и интересует:")
    if bench_rows:
        per, gate, raw, off_hist = {}, {}, {}, {}
        for asset, start, cp, b, sp, ts, bage in bench_rows:
            md = data.get(asset)
            if not md:
                continue
            mins, cl = md[0], md[1]
            b0, b1 = close_at(mins, cl, start), close_at(mins, cl, start - 60)
            if b0 is None or b1 is None or b <= 0:
                continue
            day = time.strftime("%Y-%m-%d", time.gmtime(start))
            raw.setdefault((asset, day), []).append(math.log(b / b0))
            per.setdefault((asset, day), []).append((b, b0, b1, start, bage))
            # Если bench не совпадает ни с одной из двух минут, это может быть
            # и не «фид встал», а съехавшее окно (у актива другой шаг/выравнивание)
            # или квант телеметрии. Отличаем одним движением: ищем ближайший клин
            # в окне ±3 минут и смотрим, куда приходится оптимум.
            if min(abs(b / b0 - 1), abs(b / b1 - 1)) * 1e4 > 2.0:
                best = None
                for kk in range(-3, 4):
                    cv = close_at(mins, cl, start + kk * 60)
                    if cv:
                        e = abs(b / cv - 1) * 1e4
                        if best is None or e < best[1]:
                            best = (kk, e)
                if best is not None:
                    off_hist.setdefault((asset, day), {}).setdefault(best[0], [0, []])
                    h = off_hist[(asset, day)][best[0]]
                    h[0] += 1
                    h[1].append(best[1])
        for (asset, day), v in sorted(raw.items()):
            k = math.exp(qmed(v))                       # базис суток, мультипликативный
            rows = per[(asset, day)]
            e0 = [abs(b / (k * b0) - 1) * 1e4 for b, b0, b1, st, ag in rows]
            e1 = [abs(b / (k * b1) - 1) * 1e4 for b, b0, b1, st, ag in rows]
            # Полшага квантования телеметрии в б.п. Для BTC-клиньев это 0.05 б.п.,
            # для XRP по цене ~$2.8 — 18 б.п.: «мимо обеих минут» там означает не
            # поломку, а то, что журнал не умеет различать минуты.
            qmax = max([0.005 / b * 1e4 for b, b0, b1, st, ag in rows] + [0.0])
            thr = max(3.0, qmax)
            g = {"t": 0, "p": 0, "x": 0, "last_old": 0, "last_new": 0}
            for i, (b, b0, b1, st, ag) in enumerate(rows):
                if min(e0[i], e1[i]) > thr:
                    g["x"] += 1                          # не та и не другая минута
                elif e0[i] <= e1[i]:
                    g["t"] += 1
                    g["last_new"] = max(g["last_new"], st)
                else:
                    g["p"] += 1
                    g["last_old"] = max(g["last_old"], st)
            gate[(asset, day)] = g
            m0, m1 = qmed(e0), qmed(e1)
            verdict = ("РОВНАЯ копия clobmm: close(start)" if m0 <= m1 and m0 <= 3.0
                       else ("старая конвенция: close(start−60)" if m1 < m0 and m1 <= 3.0
                             else "НИ ТА НИ ДРУГАЯ: MinuteBook отдал не ту минуту "
                                  "(фид встал или журнал старого билда)"))
            cut = ("граница билда внутри суток: старая конвенция до "
                   + time.strftime("%H:%M", time.gmtime(g["last_old"]))
                   + ", новая с окна, стартующего в "
                   + time.strftime("%H:%M", time.gmtime(g["last_new"]))
                   if g["t"] and g["p"] else
                   ("весь день на close(start)" if g["t"] and not g["p"]
                    else "весь день на close(start−60)" if g["p"] and not g["t"]
                    else "весь день мимо обеих минут"))
            print(f"   {asset} {day}: n={len(rows):>5}  базис {(k - 1) * 1e4:>+7.1f} б.п.  "
                  f"медиана расстояния до close(start) {m0:>6.3f} б.п., до "
                  f"close(start−60) {m1:>6.3f}   [новая: {g['t']}, старая: {g['p']}, "
                  f"мимо обеих: {g['x']}]  ⇒ {verdict}")
            print(f"      {cut}"
                  + (f"   (квант телеметрии {qmax:.1f} б.п. — точнее журнал не видит)"
                     if qmax > 1.0 else ""))
            oh = off_hist.get((asset, day), {})
            if oh:
                tot = sum(v[0] for v in oh.values())
                top = max(oh.items(), key=lambda kv: kv[1][0])
                if top[0] != 0 and top[1][0] >= 0.5 * tot:
                    print(f"      СЪЕЗД ОКНА: у {top[1][0]}/{tot} «мимо» ближайший клин"
                          f" на {top[0]:+d} мин (расстояние {qmed(top[1][1]):.2f} б.п.) "
                          "— у этого актива окно выровнено не по нашей минуте")
                else:
                    print(f"      среди «мимо» ближайший клин в 0 мин у "
                          f"{oh.get(0, [0])[0]}/{tot} — квант/дыра фида, не съезд окна")
            nb = sum(1 for r in rows if r[4] and r[4] >= 2)
            if nb:
                print(f"      {nb} из {len(rows)} окон движок решал по минуте "
                      f"возрастом ≥2 мин (bench_age) — эти окна в гейты не входят")
        print("   смысл: у нового билда «расстояние до close(start)» = 0.000 (ровно та "
              "минута), у старого оно было 1–2 б.п. при минимуме на close(start−60). "
              "Если минимум не на обеих колонках — движок делил не по клиновой минуте, "
              "и вчерашний вывод про +17.92¢ надо пересматривать на этих сутках.")

        # L2) Здоровье спот-фида по одному только journal (без клинов): aggTrade жив —
        # значит spot в подряд идущих интентах одного окна обязан меняться.
        hr = {}
        for asset, start, cp, b, sp, ts, bage in bench_rows:
            if not ts:
                continue
            d = hr.setdefault((asset, ts // 3600), [0, set(), set(), 0])
            d[0] += 1
            d[1].add(sp)
            d[2].add(b)
            # cp>=180 в 5m-окне даёт spot==bench ПО ОПРЕДЕЛЕНИЮ (минута та же:
            # spot=close(end-cp-60)), поэтому заморозку ищем только на поздних
            # чекпоинтах: cp<=120 = отсчёт >=3 минут от старта окна.
            if sp == b and 0 < (cp or 999) <= 120:
                d[3] += 1
        bad_hr = [k for k, v in sorted(hr.items()) if v[0] >= 20 and len(v[1]) <= 2]
        late = sum(v[3] for v in hr.values())
        tot = sum(v[0] for v in hr.values())
        print(f"\nL2) ЗДОРОВЬЕ СПОТ-ФИДА (по journal, клинья не нужны): "
              f"интентов с телеметрией {tot}, из них spot == bench при отсчёте ≥3 мин "
              f"от старта окна (не по определению) — {late} ({100.0 * late / tot:.1f}%) , часов с "
              f"≥20 интентами и ≤2 различными ценами: {len(bad_hr)}")
        for k in bad_hr[:6]:
            v = hr[k]
            print(f"      заморозка: {k[0]} "
                  f"{time.strftime('%m-%d %H:00', time.gmtime(k[1] * 3600))} — "
                  f"{v[0]} интентов, цен спота {len(v[1])}, цен бенча {len(v[2])}")
        if bad_hr:
            print("      что это значит: MinuteBook в эти часы не пополнялся, и все "
                  "x/h/метки там — по замороженной цене. Числа гейтов по таким часам "
                  "нужно переснимать с --start-after/--start-before, а/live-прогон "
                  "в эти часы не считается прогоном вовсе.")
        else:
            print("      фид живой во всех часах с телеметрией.")
    else:
        print("   в journal нет поля bench в intent — значит живой процесс старше "
              "телеметрии (правого билда нет в работе), и сверять нечем")

    # ---- M) точность ЗНАКА против площадки: сигнал или отбор убивает P&L? ----
    print("\nM) ТОЧНОСТЬ НАШЕГО ЗНАКА ПРОТИВ ПЛОЩАДКИ (не против нашей метки):")
    if not ymap:
        print("   пропущено: нужен --venue. Без него вопрос «мы плохо предсказываем"
              " или нас плохо исполняют» остаётся открытым, а A/B/F/J судят по "
              "клиновому прокси")
    else:
        def cpb(cp):
            cp = cp if cp is not None else 0
            return "≤30 с до конца" if cp <= 30 else (
                "60 с" if cp <= 60 else ("120–180 с" if cp <= 180 else "≥240 с"))
        vb = {}
        last = {}
        for i in intents:
            k = (i["asset"], i["start"])
            y, x = ymap.get(k), i.get("x")
            if y is None or x is None:
                continue
            d = vb.setdefault(cpb(i.get("cp")), [0, 0])
            d[1] += 1
            if (1 if x > 0 else 0) == y:
                d[0] += 1
            cp = i.get("cp")
            cp = 999 if cp is None else cp
            cur = last.get(k)
            if cur is None or cp < cur[0]:
                last[k] = (cp, x, y)
        nw = len(last)
        hitw = sum(1 for _cp, x, y in last.values() if (1 if x > 0 else 0) == y)
        print(f"   «view» движка (сторона котировки = знак x) против платежа: "
              f"по окнам n={nw} совпало {hitw} = {100.0 * hitw / max(nw, 1):.1f}% "
              f"(монетка 50%, SE {50.0 / max(nw, 1) ** .5:.1f} п.п.)")
        for b in sorted(vb):
            h, t = vb[b]
            if t:
                print(f"      {b:<16} {100.0 * h / t:>5.1f}%  (n={t} интентов)")
        # AUC по окнам: не «попадание знака», а ранговая предиктивность величины x
        xs = sorted(((x, y) for _cp, x, y in last.values()), key=lambda z: z[0])
        pos = [x for x, y in xs if y == 1]
        neg = [x for x, y in xs if y == 0]
        if pos and neg:
            import bisect as _bs
            u = 0.0
            for a1 in pos:
                lo_ = _bs.bisect_left([b1 for b1 in neg], a1)
                hi_ = _bs.bisect_right([b1 for b1 in neg], a1)
                u += lo_ + (hi_ - lo_) / 2.0
            auc = u / (len(pos) * len(neg))
            print(f"   AUC(x → платёж) по {nw} окнам = {auc:.3f} "
                  f"(0.5 = шума; <0.5 = знак ПЕРЕВЁРНУТ, и это эксплуатуется)")
        # сторона на ФИЛЛАХ: то, что реально купили, против платежа
        BX = ((0.0, 0.5), (0.5, 1.0), (1.0, 2.0), (2.0, 99.0))
        byx = {b: [0, 0] for b in BX}
        # то же по ВРЕМЕНИ филла внутри окна: «когда именно нас снимают». Если
        # win-rate падает к концу окна — это не «нет сигнала», а нас снимают
        # инфо́рмированные тейкеры, когда исход уже почти решён: у оракула свой
        # ~30-сек лаг, поэтому последние секунды окна для maker-заявки,
        # выставленной по спот-запаздывающей минуте, заведомо нечестные.
        CPB = ((0, 30), (30, 61), (61, 121), (121, 181), (181, 9999))
        bycp = {b: [0, 0] for b in CPB}
        for e in fills:
            k = (e["asset"], e["start"])
            y = ymap.get(k)
            if y is None:
                continue
            side = 1 if e.get("level") == "bid" else 0
            xx = None
            cpf = None
            for i in intents:
                if i["asset"] == e["asset"] and i["start"] == e["start"] \
                        and i["ts"] <= e["ts"] and i.get("x") is not None:
                    xx = i["x"]
                    cpf = i.get("cp")
            if xx is None:
                continue
            for b in BX:
                if b[0] <= abs(xx) < b[1]:
                    byx[b][1] += 1
                    byx[b][0] += 1 if side == y else 0
                    break
            if cpf is not None:
                for b in CPB:
                    if b[0] <= cpf < b[1]:
                        bycp[b][1] += 1
                        bycp[b][0] += 1 if side == y else 0
                        break
        print("  win-rate НАШИХ ФИЛЛОВ против платежа, по силе сигнала |x|:")
        for b in BX:
            h, t = byx[b]
            if t:
                print(f"      |x| {b[0]:.1f}–{b[1]:.1f}  n={t:>3}  win {100.0 * h / t:>5.1f}%  "
                      f"(SE {50.0 / t ** .5:.1f} п.п.)")
        print("   win-rate филлов по МОМЕНТУ внутри окна (cp = секунд до конца):")
        for b in CPB:
            h, t = bycp[b]
            if t:
                print(f"      cp {b[0]:>3}–{b[1]:<4} n={t:>3}  win {100.0 * h / t:>5.1f}%  "
                      f"(SE {50.0 / t ** .5:.1f} п.п.)")
        print("   чтение: «view» отвечает на вопрос «знаем ли мы направление», а эта "
              "таблица — на вопрос «кого мы набираем по своим заявкам». Если view ≈50% "
              "и филлы ≈50% — рынка нет ни в какую сторону, и вся медиана markout "
              "покупается спредом/рибейтом. Если view выше 50%, а филлы ниже — нас "
              "съедает ОТБОР (ставка стоит, движение уходит: stale-котировка), и "
              "чинится это латентностью/отменами, а не моделью. Если view НИЖЕ 50% "
              "устойчиво — знак перевёрнут, и это единственный случай, когда "
              "предсказание вообще что-то меняет в решение-центре.")
        # Статистика, а не «на глаз»: view против монеты, и филлы против view.
        fh = sum(v[0] for v in byx.values())
        ft = sum(v[1] for v in byx.values())
        z_view = ((hitw - nw / 2.0) / (nw ** .5 / 2.0)) if nw else float("nan")
        if nw and ft:
            pv, pf = hitw / nw, fh / ft
            se = (pv * (1 - pv) / nw + pf * (1 - pf) / ft) ** .5
            z_dif = (pv - pf) / se if se > 0 else float("nan")
        else:
            pf, z_dif = float("nan"), float("nan")
        print(f"   статистика: view против монеты z = {z_view:+.2f}; "
              f"win-rate филлов ({100 * pf:.1f}% на n={ft}) против view "
              f"z = {z_dif:+.2f}")
        if not nw or ft < 20:
            print("   вывод не делается: мало окон/филлов с x и venue-y одновременно")
        elif z_view <= -2:
            print("   ⇒ ЗНАК ПЕРЕВЁРНУТ (view ниже монеты на 2σ+). Это единственный "
                  "случай, когда правка ЦЕНТРА котирования обоснована числами; "
                  "до неё — ничего не трогаем, потому что перевёрнутый знак = "
                  "положительный сигнал, и гейты надо переснимать, а не чинить σ.")
        elif z_view >= 2 and z_dif >= 2:
            print("   ⇒ сигнал есть, но его съедает ОТБОР: мы набираем худшие стороны. "
                  "Чинится латентностью и отменами (M6/M7), а не моделью; правки "
                  "величины x тут бесполезны.")
        elif z_view >= 2:
            print("   ⇒ сигнал есть, отбор не доказан (разница с филлами < 2σ) — "
                  "смотреть ёмкость/очки M6.")
        else:
            print("   ⇒ ни сигнала, ни отбора: view = монета, и весь markout "
                  "покупается спредом и рибейтом. Правки модели по этим данным "
                  "обоснованы быть не могут — это и есть ответ «почему не profit». ")
        print("   надёжность: интенты внутри окна скоррелированы, поэтому «view» "
              "считается ПО ОДНОМУ разу на окно (последний чекпоинт); перед выводами "
              "проверь устойчивость на срезах --start-after/--start-before по суткам")

    # ---- маргинальные филлы: стоит ли брать «бесплатную» точность σ ----
    thr = a.flip_margin * 0.5     # 1.6% от типичного h≈0.5¢ = 0.008¢
    LADDER = (0.01, 0.03, 0.05, 0.10, 0.25)
    dist_fill, near, bad_join = [], 0, 0
    for e in fills:
        b, ask, mid = None, None, e.get("mid")
        for i in intents:
            if i["asset"] == e["asset"] and i["start"] == e["start"] and i["ts"] <= e["ts"]:
                b, ask = i["bid"], i["ask"]
        if b is None or mid is None:
            continue
        dist = (b - mid) * 100 - F_CENTS if e["level"] == "bid" else (mid - ask) * 100 - F_CENTS
        dist_fill.append(dist)
        if abs(dist) < thr:
            near += 1
        if dist < -0.02:
            bad_join += 1
    if dist_fill:
        ds = sorted(dist_fill)
        print(f"\nD) Чувствительность филлов к точности σ (запас над линией срабатывания):")
        print(f"   n={len(ds)}  медиана запаса = {qmed(ds):.3f}¢   "
              f"при ±{100 * a.flip_margin:.1f}% σ линия сдвигается на {thr:.3f}¢")
        for L in LADDER:
            sh = 100 * sum(1 for x in ds if x < L) / len(ds)
            flag = "  (≈ шаг квантования journal, ниже не измеряемо)" if L <= 0.01 else ""
            print(f"   доля филлов с запасом < {L:>5.2f}¢ = {sh:>5.1f}%{flag}")
        print(f"   прямая оценка «на волоске» (< {thr:.3f}¢) = {100 * near / len(ds):.1f}%.")
        print(f"   sanity джойна intent↔fill: филлов с запасом < −0.02¢ = {bad_join} "
              +("(должно быть ~0, иначе связка.quote→fill сбита и D не читать)" if bad_join else "( OK )"))
        print("   Ограничение: обратную сторону (какие НЕ-филлы стали бы филлами при")
        print("   более широком h) journal не содержит — там нет mid книги на каждый")
        print("   intent. Это единственная часть вывода про σ, которую калькулятор не")
        print("   закрывает; она требует clob-лога (расчётная машина, 21.09).")
    if a.csv:
        with open(a.csv, "w") as f:
            for r in rows_out:
                f.write(json.dumps(r) + "\n")
        print(f"\ncsv: {a.csv} ({len(rows_out)} строк)")


if __name__ == "__main__":
    main()
