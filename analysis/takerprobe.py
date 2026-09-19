#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""takerprobe.py — есть ли механический эдж у ТЕЙКЕРА в конце updown-окна.

Вопрос ставится ровно так, как его решает тейкер, а не мы-мейкеры: в момент
t = конец_окна − cp посмотреть, какая сторона стоит ВЫШЕ цены отсчёта площадки
(ptb), и ВЗЯТЬ эту сторону по её же топу (Up — по ask, Down — по 1−bid). Если
фактическая частота выигрыша этой стороны систематически выше цены, которую за
неё просят, эдж есть и живёт он в тайминге, а не в прогнозе; если нет — тогда
«тейкеры последних секунд» зарабатывают не на критерии входа, а на чём-то ещё
(скорость отмены у мейкера, инвентарь, рибейт), и нашу медиану markout правкой
сигнала не вылечить.

Метка — ПЛАТЁЖ площадки (finalPrice ≥ priceToBeat из venue-кэша markcalc), не наш
клиновый прокси: тейкерский эдж измеряется только против того, за что платят
(см. docs/roadmap-to-profit.md, блок K).

Два критерия входа сравниваются между собой:
  spot — мгновенная цена в t (то, что обычно и подразумевают под «цена выше страйка»);
  twap — среднее по ВИДИМОЙ части разрешающего окна [end−60, t] (правило резолва =
         TWAP60 по gamma-метаданным рынка, см. тот же док). При cp > 60 разрешающее
         окно ещё не началось и twap вырождается в spot — это отмечено в выводе.

Информация — строго до t. Без 1-секундных клинов берётся закрытие последней
ЗАКРЫВШЕЙСЯ минуты (strict; ровно как в живом движке) и, для контраста, вариант
naive, который подглядывает в текущую минуту до 59 с — он приводится только чтобы
показать цену самоналоженной задержки, выводов по нему не делать. С 1-секундными
клинами (--secs-cache, докачка с api.binance.com) разрешение = 1 с, и тогда
последние 5–30 секунд измеряемы вообще; без них этот вопрос неизмерим в принципе.

    python3 takerprobe.py --win /tmp/win --venue /tmp/venue.jsonl --code 5m
    python3 takerprobe.py --win /tmp/win --venue /tmp/venue.jsonl --secs-cache /tmp/kl1s.jsonl
    python3 takerprobe.py --selftest

Ограничения, которые надо держать в голове, читая таблицы: топ книги на 1 акцию
(глубины в датасете нет), цена в книге заморожена на чекпоинтах по ПОСЛЕДНЕМУ
обновлению книги (т.е. чуть старше t), а Δ ниже ~2 б.п. лежит внутри шума
«Chainlink против Binance» (замерено: 0.81 б.п. медианой в метку времени).
"""
import argparse
import csv
import glob
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request

SYMS = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "xrp": "XRPUSDT"}
GAPS = ((0.0, 1.0), (1.0, 2.0), (2.0, 3.0), (3.0, 5.0), (5.0, 8.0),
        (8.0, 15.0), (15.0, 30.0), (30.0, 1e9))
PRICES = ((0.0, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9),
          (0.9, 0.97), (0.97, 1.0001))
DELTA = (0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 15.0, 30.0)


def mean(v):
    return sum(v) / len(v) if v else float("nan")


def z_vs0(v):
    """t-статистика среднего против нуля (по акциям-входам, не по окнам)."""
    n = len(v)
    if n < 4:
        return float("nan")
    m = mean(v)
    sd = math.sqrt(sum((x - m) ** 2 for x in v) / (n - 1))
    return m / (sd / math.sqrt(n)) if sd > 0 else float("nan")


def last_minute(mins, t, naive=False):
    """Закрытие последней минуты, ЗАКРЫВШЕЙСЯ до t (naive — минуты, содержащей t)."""
    base = (t // 60) * 60
    j0 = 0 if naive else 1
    for j in range(j0, j0 + 6):
        k = base - 60 * j
        if not naive and k + 60 > t:
            continue
        if k in mins:
            return mins[k]
    return None


# ---------------------------------------------------------------- вход
def load_windows(win_dir, assets, code, cps, daysel=None):
    out = []
    for path in sorted(glob.glob(os.path.join(win_dir, "windows_*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                if r["asset"] not in assets or r["code"] != code:
                    continue
                st0 = int(float(r["start"]))
                if daysel and time.strftime("%Y%m%d", time.gmtime(st0)) not in daysel:
                    continue
                slot, sz = {}, {}
                for c in cps:
                    b = (r.get("b%d" % c) or "").strip()
                    a = (r.get("a%d" % c) or "").strip()
                    if b and a:
                        try:
                            slot[c] = (float(b), float(a))
                        except ValueError:
                            pass
                for c in cps:      # глубина в 2¢ от топа: появилась в clobwin вместе
                    d2b = (r.get("d2b%d" % c) or "").strip()   # с инвентарными
                    d2a = (r.get("d2a%d" % c) or "").strip()   # прогонами
                    if d2b and d2a:
                        try:
                            sz[c] = (float(d2b), float(d2a))
                        except ValueError:
                            pass
                if not slot:
                    continue
                out.append({"start": int(float(r["start"])), "end": int(float(r["end"])),
                            "asset": r["asset"], "vol": float(r.get("vol_usd") or 0),
                            "n_pc": int(float(r.get("n_pc") or 0)), "slot": slot,
                            "sz": sz})
    out.sort(key=lambda w: (w["asset"], w["start"]))
    return out


def load_minutes(win_dir, assets):
    m = {a: {} for a in assets}
    for path in sorted(glob.glob(os.path.join(win_dir, "minutes_*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                a = r["asset"]
                if a in m:
                    try:
                        m[a][int(float(r["minute"]))] = float(r["close"])
                    except (ValueError, KeyError):
                        pass
    return m


def load_venue(path, code):
    out = {}
    if not path or not os.path.exists(path):
        return out
    with open(path) as f:
        for ln in f:
            try:
                r = json.loads(ln)
            except ValueError:
                continue
            if r.get("code") not in (None, code):
                continue
            if r.get("ptb") is None or r.get("fp") is None:
                continue
            y = r.get("y")
            y = 1 if r["fp"] >= r["ptb"] else 0 if y is None else int(y)
            out[(r["asset"], int(r["start"]))] = {"ptb": float(r["ptb"]),
                                                   "fp": float(r["fp"]), "y": y}
    return out


def fetch_secs(windows, venue, cache_path, sleep=0.05, verbose=True,
               workers=6, rate=45.0, max_windows=10 ** 9):
    """~1 HTTP-запрос на окно, пул из `workers` потоков и общий ограничитель темпа.

    15k окон последовательно — это 20+ минут простоя в сеть; шесть потоков берут это
    за ~5 минут, а темп (по умолчанию 45 запросов/с) держит нас под лимитом Binance
    по весу (6000 weight/мин, 1с-клины = вес 2 => ~50 req/s потолок), потому что за
    превышение банят IP, а не ретраят.
    """
    """1-секундные клины Binance на [start, end) для окон, где есть платёж.

    Кэш append-only jsonl: {"asset","start","t0","c":[...]} — повторный прогон
    бесплатен. Сеть недоступна -> тихий отказ и работа по минутам."""
    have = {}
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            for ln in f:
                try:
                    r = json.loads(ln)
                    have[(r["asset"], r["start"])] = r
                except ValueError:
                    pass
    todo = [w for w in windows if (w["asset"], w["start"]) in venue
            and (w["asset"], w["start"]) not in have]
    todo = todo[:max_windows]
    if verbose:
        print(f"1с-клины: в кэше {len(have)}, докачать {len(todo)}", file=sys.stderr)
    if not todo:
        return have
    todo = [w for w in todo if w["asset"] in SYMS]

    def get(w):
        url = ("https://api.binance.com/api/v3/klines?symbol=%s&interval=1s"
               "&startTime=%d&limit=300" % (SYMS[w["asset"]], w["start"] * 1000))
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                rows = json.loads(r.read().decode())
        except (urllib.error.URLError, OSError, ValueError):
            return None
        c = [float(x[4]) for x in rows if len(x) > 4]
        if len(c) < 240:
            return None
        return {"asset": w["asset"], "start": w["start"], "t0": w["start"], "c": c}

    from concurrent.futures import ThreadPoolExecutor
    per = max(1, min(len(todo), 12 * workers))
    t_next = time.monotonic()
    dt = 1.0 / max(rate, 1.0)
    try:
        with open(cache_path, "a") as f:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                for i0 in range(0, len(todo), per):
                    ch = todo[i0:i0 + per]
                    n0 = time.monotonic()
                    got = list(ex.map(get, ch))
                    for rec in got:
                        if rec is None:
                            continue
                        have[(rec["asset"], rec["start"])] = rec
                        f.write(json.dumps(rec) + "\n")
                    f.flush()
                    ok = sum(1 for x in got if x is not None)
                    print(f"  1с-клины: {min(i0 + per, len(todo))}/{len(todo)} "
                          f"(ок {ok}/{len(ch)})", file=sys.stderr, flush=True)
                    if ok == 0:
                        print("1с-клины: пустые ответы (сеть/451/лимит) — перехожу "
                              "на минуты", file=sys.stderr)
                        break
                    # общий темп: пул не должен выдать больше rate запросов в секунду
                    wait = t_next + dt * len(ch) - time.monotonic()
                    t_next = max(t_next + dt * len(ch), time.monotonic())
                    if wait > 0:
                        time.sleep(wait)
    except OSError as e:
        print(f"1с-клины: кэш недоступен ({e}), работаем по минутам", file=sys.stderr)
    return have


# ---------------------------------------------------------------- ядро
def build_entries(windows, mins, secs, venue, cps, fee, use_secs):
    """Одна строка на (окно, чекпоинт, критерий). Никакого подглядывания: в t
    доступны только закрытые минуты / закрытые секунды и книга до t."""
    ent = []
    n_cov = 0
    for w in windows:
        v = venue.get((w["asset"], w["start"]))
        if not v:
            continue
        n_cov += 1
        ptb, y, end = v["ptb"], v["y"], w["end"]
        for c in cps:
            s = w["slot"].get(c)
            if not s:
                continue
            bid, ask = s
            t = end - c
            sec = None
            if use_secs:
                r = secs.get((w["asset"], w["start"]))
                if r and r["t0"] == w["start"]:
                    arr = r["c"]
                    i = int(t) - r["t0"] - 1          # последняя ЗАКРЫТАЯ секунда до t
                    if 0 <= i < len(arr):
                        sec = arr[i]
                        tw = [x for x in arr[max(0, int(end - 60) - r["t0"]):i + 1]]
                        tw = mean(tw) if len(tw) >= 10 else None
                    else:
                        tw = None
                    spot = sec
                else:
                    spot = last_minute(mins.get(w["asset"], {}), t)
                    tw = None
            else:
                spot = last_minute(mins.get(w["asset"], {}), t)
                tw = None
            if spot is None or ptb <= 0:
                continue
            sz = w.get("sz") or {}
            for sig, val in (("spot", spot), ("twap", tw if tw is not None else spot)):
                if val is None:
                    continue
                gap = (val / ptb - 1.0) * 1e4
                lead = 1 if gap > 0 else 0
                p = ask if lead == 1 else (1.0 - bid)
                if not (0.0 < p < 1.0):
                    continue
                win = 1 if lead == y else 0
                ev = (100.0 - p * 100.0) if win else (-p * 100.0)
                ent.append(dict(asset=w["asset"], start=w["start"], end=end, cp=c, sig=sig,
                                gap=round(gap, 3), lead=lead, side=("Up" if lead else "Down"),
                                p=round(p, 4), win=win, ev=round(ev - fee, 3),
                                bid=bid, ask=ask, vol=w["vol"], n_pc=w["n_pc"],
                                # для ТЕЙКЕРА отображённая глубина = то, что он съест
                                # (up: ask-сторона; down: наши же bids как ask NO-токена)
                                avail=(sz.get(c) or (None, None))[1 if lead == 1 else 0],
                                secs=use_secs and sec is not None,
                                tw_real=1 if (sig == "twap" and tw is not None) else 0))
    return ent, n_cov


def report(ent, n_win, n_cov, args, use_secs, a_split=False):
    def head(t):
        print("\n" + t)

    have5 = sorted({r["cp"] for r in ent})
    print(f"окон в датасете {n_win}, с платежом площадки {n_cov}; входов "
          f"{len(ent)} (пополам по критериям), разрешение: "
          + ("1 СЕКУНДА" if use_secs else "МИНУТА (последние секунды неизмеримы!)"))
    if n_cov < 0.3 * max(n_win, 1):
        print("   ВНИМАНИЕ: платёж площадки есть меньше чем для 30% окон — таблицы "
              "будут по скошенному набору (обычно это окна, в которых мы не стояли). "
              "Сначала докачай платёж по всему датасету:")
        print('     python3 /tmp/markcalc.py --venue-keys %s --venue-cache '
              '/tmp/venue.jsonl --venue-workers %d%s'
              % (args.win, max(1, args.workers),
                 (' --venue-key-days ' + args.days) if args.days else ""))
    if not ent:
        print("  входов нет: проверь, что --win построен за те же сутки, что и --venue")
        return {}
    days = sorted({time.strftime("%Y-%m-%d", time.gmtime(r["start"])) for r in ent})
    print("   сутки в выборке: " + ", ".join(days) + " | доля окон с книгой и "
          f"платежом = {100.0 * n_cov / max(n_win, 1):.0f}% (остальное — окна, где "
          "clobwin не нашёл книги: пустой стакан или нет токена в карте)")
    sigs = ("spot",) if not use_secs else ("spot", "twap")
    if not use_secs:
        print("   (критерий twap без 1с-клинов неотличим от spot — печатается одна "
              "таблица; за --secs-cache, иначе вопрос «последние секунды» неизмерим)")
    for sig in sigs:
        rows = [r for r in ent if r["sig"] == sig]
        if not rows:
            continue
        head(f"Т1. Критерий «{sig}»: брать сторону, ведущую против ptb (без фильтра по силе)")
        print(f"   {'за N с до конца':<18} {'n':>5} {'win%':>7} {'цена':>7} "
              f"{'ошибка':>8} {'EV ¢/акц':>9} {'z':>6}  {'окон':>5}")
        print(f"   {'':<18} {'':>5} {'':>7} {'':>7} {'(win−цена)':>8}")
        for c in have5:
            v = [r for r in rows if r["cp"] == c]
            if len(v) < args.min_n:
                continue
            wr = 100.0 * mean([r["win"] for r in v])
            pp = mean([r["p"] for r in v])
            print(f"   cp = {c:<14} {len(v):>5} {wr:>6.1f}% {pp:>7.3f} "
                  f"{100.0 * (wr / 100.0 - pp):>+7.1f}% {mean([r['ev'] for r in v]):>9.2f} "
                  f"{z_vs0([r['ev'] for r in v]):>6.2f}  "
                  f"{len({(r['asset'], r['start']) for r in v}):>5}")
    head("Т2. EV по ВРЕМЕНИ × СИГНАЛУ (|отрыв от ptb| в б.п.), критерий twap, ¢/акц")
    base = [r for r in ent if r["sig"] == "twap"]
    print("   cp\\|gap| " + "".join(f"{lo:g}{'' if hi < 1e8 else '+'}".rjust(9)
                                   for lo, hi in GAPS))
    for c in have5:
        line = f"   {c:>7} "
        for lo, hi in GAPS:
            v = [r["ev"] for r in base if r["cp"] == c and lo <= abs(r["gap"]) < hi]
            line += (f"{mean(v):>8.1f} " if len(v) >= args.min_n else "        · ")
        print(line)
    head("Т3. EV по ВРЕМЕНИ × ЦЕНЕ, за которую мы бы заплатили (twap, ¢/акц)")
    print("   cp\\цена  " + "".join(f"{lo:.2f}–{min(hi, 1.0):.2f}".rjust(11)
                                   for lo, hi in PRICES))
    for c in have5:
        line = f"   {c:>7} "
        for lo, hi in PRICES:
            v = [r["ev"] for r in base if r["cp"] == c and lo <= r["p"] < hi]
            line += (f"{mean(v):>7.1f}({len(v)}) " if len(v) >= max(4, args.min_n // 2)
                     else "         ·  ")
        print(line)
    head("Т4. Перебор критериев «когда и по какой цене входить»: (cp, Δ, потолок цены)")
    best = []
    for c in have5:
        for d in DELTA:
            prev_n = None
            for cap in (1.0001, 0.95, 0.90, 0.80, 0.70):
                v = [r for r in base if r["cp"] == c and abs(r["gap"]) >= d and r["p"] <= cap]
                if len(v) < args.min_n or len(v) == prev_n:
                    continue     # потолок цены ничего не отсекает — строка не нужна
                prev_n = len(v)
                ev = mean([r["ev"] for r in v])
                if args.inv_usd > 0:
                    kn = [r for r in v if r.get("avail") is not None]
                    ex = (100.0 * mean([1.0 if r["avail"] * r["p"] * 100.0
                                        >= args.inv_usd else 0.0 for r in kn])
                          if kn else None)   # None = глубины в датасете нет
                else:
                    ex = float("nan")
                best.append((ev, z_vs0([r["ev"] for r in v]), len(v), c, d, cap,
                             100.0 * mean([r["win"] for r in v]),
                             mean([r["p"] for r in v]),
                             len({(r["asset"], r["start"]) for r in v}), ex,
                             len(kn)))
    best.sort(reverse=True)
    hdr = (f"   {'EV ¢':>7} {'z':>6} {'n':>5} {'cp':>5} {'Δб.п.':>6} {'цена≤':>6} "
           f"{'win%':>7} {'цена':>6} {'окон':>5}")
    if args.inv_usd > 0:
        hdr += f" {'по объёму':>12}"
    print(hdr)
    for ev, z, n, c, d, cap, wr, pp, nw, ex, knd in best[:12]:
        line = (f"   {ev:>7.2f} {z:>6.2f} {n:>5} {c:>5} {d:>6g} {min(cap, 1.0):>6.2f} "
                f"{wr:>6.1f}% {pp:>6.3f} {nw:>5}")
        if args.inv_usd > 0:
            # без knd «100%» на двух пересобранных сутках читалось как «весь набор»
            line += "        н/д" if ex is None else f" {ex:>7.0f}%({knd})"
        print(line)
    if args.inv_usd > 0 and all(b[9] is None for b in best[:12]):
        print("   колонка «по объёму» = н/д: в датасете нет колонок d2b{cp}/d2a{cp} — "
              "пересобери окна свежим clobwin (он пишет глубину с этого коммита)")
    elif args.inv_usd > 0:
        print(f"   «по объёму» = доля входов, где отображённая глубина в 2¢ от топа "
              f"вместила бы ${args.inv_usd:.0f} ПО ПОКАЗАННОЙ ЦЕНЕ; в скобках — знаменатель "
              f"(входы с известной глубиной: только пересобранные сутки, где clobwin писал "
              f"d2*). Это ВЕРХНЯЯ граница: ни очереди, ни того, что стакан в последнюю "
              f"секунду снимают быстрее, чем летит FOK, она не знает.")
    pos = [b for b in best if b[0] > 0 and b[1] >= 2]
    print("\n   ДИАГНОЗ: " + (
        f"эдж есть в {len(pos)} конфигурациях (z≥2). Лучшая: cp={pos[0][3]} с, "
        f"Δ≥{pos[0][4]:g} б.п., цена≤{min(pos[0][5], 1.0):.2f} → EV {pos[0][0]:.2f} ¢/акц "
        f"на {pos[0][2]} входах / {pos[0][8]} окон. Дальше считать ЁМКОСТЬ и "
        "устойчивость по суткам, а не радоваться." if pos else
        "ни одна конфигурация входа не даёт EV>0 на 2σ. Значит «тейкеры последних "
        "секунд» в этой истории зарабатывают НЕ на критерии входа по споту/TWAP "
        "против ptb — и нашим markout-дефектом это тоже не объясняется."))
    if a_split:
        # Порог в б.п. нельзя сравнивать между активами: сигма окна у sol/xrp в 2–3
        # раза выше, чем у btc (урок 3 в measured-signal-baseline.md). Поэтому сетка Δ
        # прогоняется отдельно по каждому активу — «эдж в 5 б.п.» для btc и для xrp
        # это ДВЕ РАЗНЫЕ ставки на одно и то же событие.
        head("Т4a. То же по активам (Δ и потолок — внутри актива; сигмы несравнимы)")
        for ast in sorted({r["asset"] for r in base}):
            rows = [r for r in base if r["asset"] == ast]
            bb = []
            for c in have5:
                for d in DELTA:
                    for cap in (1.0001, 0.90, 0.80):
                        v = [r for r in rows if r["cp"] == c and abs(r["gap"]) >= d
                             and r["p"] <= cap]
                        if len(v) < max(6, args.min_n // 2):
                            continue
                        bb.append((mean([r["ev"] for r in v]),
                                   z_vs0([r["ev"] for r in v]), len(v), c, d,
                                   min(cap, 1.0),
                                   len({r["start"] for r in v})))
            bb.sort(reverse=True)
            print(f"   {ast}: " + ("; ".join(
                f"cp={x[3]} Δ≥{x[4]:g} цена≤{x[5]:.2f} → {x[0]:+.1f}¢ (z={x[1]:.1f}, "
                f"n={x[2]}/{x[6]} окон)" for x in bb[:2]) if bb else "нет ячеек с n≥min"))
    if best and best[0][0] > 0:
        top = best[0]
        per_day = {}
        dollars = 0.0
        for r in base:
            if r["cp"] == top[3] and abs(r["gap"]) >= top[4] and r["p"] <= top[5]:
                k = time.strftime("%Y-%m-%d", time.gmtime(r["start"]))
                per_day.setdefault(k, []).append(r["ev"])
                dollars += r["ev"] / 100.0 * (args.bet_usd / max(r["p"], 1e-6))
        # «устойчивость по суткам» из ограничения (4): одних чисел входов мало —
        # день с 2 входами и EV −40¢ не виден как «минус», если он показан цифрой 2.
        # Ряд печатается по ВСЕМ суткам набора (0 = ни одного входа), медиана EV —
        # знак дня по входу, не по сумме (иначе один крупный выигрыш топит дыру).
        days_all = sorted({time.strftime("%Y-%m-%d", time.gmtime(r["start"]))
                           for r in base})
        nd = len(days_all) or 1
        cnt = sum(len(v) for v in per_day.values()) / nd

        def day_str(d):
            v = per_day.get(d)
            if not v:
                return "0"
            med = sorted(v)[len(v) // 2]
            return f"{len(v)} EV{'+' if med > 0 else '-'}{abs(med):.0f}"
        print(f"   масштаб: {cnt:.1f} входов/сутки, EV {top[0]:.2f} ¢/акц при ставке "
              f"${args.bet_usd:.0f} на вход (акций = ставка/цену) = "
              f"${dollars / nd:.2f}/сутки на историческом наборе из {n_cov} шт. "
              f"По всем суткам (0 = ни одного входа; EV — медиана на вход): "
              + ", ".join(f"{d[-5:]}:{day_str(d)}" for d in days_all)
              + " — и это ВЕРХНЯЯ граница: без глубины, без очереди, без отказов.")
    if use_secs and base:
        real = mean([r["tw_real"] for r in base]) * 100
        print(f"   доля входов, где twap действительно посчитан по видимой части "
              f"разрешающего окна (остальное = выродилось в spot, cp>60): {real:.0f}%")
    vols = sorted(r["vol"] for r in base)
    if vols:
        print(f"   ёмкость-прокси: оборот окна (USDT, из clob-лога) медиана "
              f"{vols[len(vols)//2]:.0f}, p10 {vols[len(vols)//10]:.0f} — на 5-минутном "
              f"окне это и есть весь рынок; топ книги на 1 акцию, глубины в датасете нет")
    anti = []
    for r in base:
        pa = (1.0 - r["bid"]) if r["lead"] == 1 else r["ask"]
        if not (0.0 < pa < 1.0):
            continue
        aw = 1 - r["win"]
        anti.append((100.0 - pa * 100.0) if aw else (-pa * 100.0))
    if anti:
        print(f"   контроль (тот же момент, та же книга, но взята ПРОИГРЫВАЮЩАЯ сторона, "
              f"без фильтра): {mean(anti) - args.fee_cents:+.2f} ¢/акц на n={len(anti)}. "
              f"Смысл: если разрыв между «вести» и «отстать» мал, то никакой критерий "
              f"из этой таблицы не работает — работает только цена входа.")
    print("   ОГРАНИЧЕНИЯ: (1) цена = топ книги на последнем обновлении ДО t, без "
          "глубины и без очереди; (2) Δ<2 б.п. внутри шума «Chainlink vs Binance» "
          "(0.81 б.п. медианой), т.е. такие ячейки — не эдж; (3) фильтр по цене "
          "использует ту же книгу, что и вход, — это не подглядывание (цена доступна "
          "в t), но потолок цены надо держать ДО решения, иначе таблица Т3 = self-fulfilling; "
          "(4) сутки берутся целиком, никаких «хороших дней» — при делении на "
          "сутки смотри, чтобы знак EV держался во всех, а не в большинстве.")
    return {"best": best[:12], "n_ent": len(ent), "n_cov": n_cov}


# ---------------------------------------------------------------- selftest
def selftest():
    """Арифметика и дисциплина «строго до t» на рукотворном окне (без сети)."""
    S = 1700000100                     // 300 * 300
    E = S + 300
    cps = [240, 120, 60, 30, 5]
    mins = {"btc": {}}
    for i in range(-3, 7):
        mins["btc"][S + i * 60] = 100.0 + 0.1 * (i + 3)     # 99.7 .. 100.6
    windows = [{"start": S, "end": E, "asset": "btc", "vol": 500.0, "n_pc": 10,
                 "slot": {240: (0.49, 0.51), 120: (0.55, 0.60), 60: (0.60, 0.65),
                          30: (0.85, 0.90), 5: (0.98, 0.99)}}]
    venue = {("btc", S): {"ptb": 100.0, "fp": 100.5, "y": 1}}
    ent, n_cov = build_entries(windows, mins, {}, venue, cps, 0.5, False)
    assert n_cov == 1
    r = [x for x in ent if x["cp"] == 120 and x["sig"] == "spot"][0]
    # t = E-120 = S+180; возьмёмся ЗАКРЫВШЕЙСЯ минуты S+120 (close 100.5), а не
    # текущей S+180 (100.6): это и есть дисциплина «строго до t», на ней держится
    # весь наш паритет sim/live. 50 б.п. против ptb=100.
    assert r["gap"] == 50.0, r
    assert r["lead"] == 1 and r["p"] == 0.60 and r["win"] == 1
    assert abs(r["ev"] - (100.0 - 60.0 - 0.5)) < 1e-9, r
    # t = E-30 = S+270 → закрытая минута S+180 = 100.6 → +60 б.п., ask 0.90 → EV 9.5
    r2 = [x for x in ent if x["cp"] == 30 and x["sig"] == "spot"][0]
    assert r2["gap"] == 60.0 and abs(r2["ev"] - 9.5) < 1e-9, r2
    # дыра в минутках: S+180 пропала -> must откатиться на S+120 (50 б.п.), а не
    # подставить None (и не схватить S+240, которая ещё не закрылась)
    m2 = dict(mins["btc"])
    del m2[S + 180]
    e_hole, _ = build_entries(windows, {"btc": m2}, {}, venue, cps, 0.5, False)
    rh = [x for x in e_hole if x["cp"] == 30 and x["sig"] == "spot"][0]
    assert rh["gap"] == 50.0, rh
    # окно, где площадка заплатила DOWN, а спот вёл UP: карательная строка
    windows2 = [dict(windows[0])]
    venue2 = {("btc", S): {"ptb": 100.0, "fp": 99.9, "y": 0}}
    e2, _ = build_entries(windows2, mins, {}, venue2, cps, 0.5, False)
    r3 = [x for x in e2 if x["cp"] == 120 and x["sig"] == "spot"][0]
    assert r3["win"] == 0 and abs(r3["ev"] - (-60.5)) < 1e-9, r3
    # 1-секундный режим: twap по видимой части разрешающего окна
    secs = {("btc", S): {"asset": "btc", "start": S, "t0": S,
                         "c": [100.0 + 0.001 * i for i in range(300)]}}
    e3, _ = build_entries(windows, mins, secs, venue, cps, 0.5, True)
    # cp=60: t = E-60 = S+240, разрешающее окно [S+240, S+300) ещё не началось
    # -> twap вырождается в spot последней закрытой секунды = c[239] = 100.239
    r4 = [x for x in e3 if x["cp"] == 60 and x["sig"] == "twap"][0]
    assert abs(r4["gap"] - 23.9) < 1e-6 and r4["tw_real"] == 0, r4
    # cp=30: t = S+270, видимая часть = c[240..269], среднее = 100.2545 -> 25.45 б.п.
    r5 = [x for x in e3 if x["cp"] == 30 and x["sig"] == "twap"][0]
    assert abs(r5["gap"] - 25.45) < 1e-6 and r5["tw_real"] == 1, r5
    assert r5["p"] == 0.90 and r5["win"] == 1 and abs(r5["ev"] - 9.5) < 1e-9, r5
    print("selftest: OK (7 проверок: строгая минута, откат по дыре, знак, EV, "
          "наказание проигравшего, вырождение twap, 1с-режим)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--win", default="", help="outdir clobwin (windows_*.csv, minutes_*.csv)")
    ap.add_argument("--venue", default="/tmp/venue.jsonl", help="кэш платежей markcalc")
    ap.add_argument("--code", default="5m", choices=["5m", "15m", "4h"])
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    ap.add_argument("--cps", default="", help="по умолчанию чекпоинты кода: 5m -> 240,120,60,30,15,5,0")
    ap.add_argument("--fee-cents", type=float, default=0.5,
                    help="тейкер-штраф ¢/акц (конвенция гейта M1; 0 = «на тоненького»)")
    ap.add_argument("--secs-cache", default="", help="путь для кэша 1с-клинов (пусто = по минутам)")
    ap.add_argument("--max-windows", type=int, default=400, help="сколько окон докачивать 1с")
    ap.add_argument("--min-n", type=int, default=8)
    ap.add_argument("--bet-usd", type=float, default=15.0)
    ap.add_argument("--split-asset", action="store_true",
                    help="сетка Δ отдельно по каждому активу (сигмы несравнимы)")
    ap.add_argument("--days", default="",
                    help="ограничить сутки: 20260904-20260914,20260917-20260918")
    ap.add_argument("--workers", type=int, default=6,
                    help="потоков на докачку 1с-клинов (сеть, не CPU)")
    ap.add_argument("--inv-usd", type=float, default=0.0,
                    help="проверить, вместила бы $N по ОТОБРАЖЁННОЙ глубине (0 = "
                         "колонка не печатается; нужны d2b/d2a в датасете)")
    ap.add_argument("--rate", type=float, default=45.0,
                    help="потолок запросов/с к Binance (вес 6000/мин => ~50)")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        selftest()
        return
    if not a.win:
        ap.error("--win обязателен (outdir clobwin с windows_*.csv и minutes_*.csv)")
    cps = ([int(x) for x in a.cps.split(",")] if a.cps
           else {"5m": [240, 120, 60, 30, 15, 5, 0],
                 "15m": [600, 240, 120, 60, 30, 15, 5, 0],
                 "4h": [1800, 600, 240, 120, 60, 30, 15, 5, 0]}[a.code])
    assets = set(x.strip() for x in a.assets.split(",") if x.strip())
    daysel = set()
    if a.days:
        import datetime as dtm
        for part in a.days.split(","):
            if "-" in part:
                x, y = part.split("-")
                d0 = dtm.datetime.strptime(x.strip(), "%Y%m%d")
                d1 = dtm.datetime.strptime(y.strip(), "%Y%m%d")
                while d0 <= d1:
                    daysel.add(d0.strftime("%Y%m%d")); d0 += dtm.timedelta(days=1)
            else:
                daysel.add(part.strip())
    windows = load_windows(a.win, assets, a.code, cps, daysel)
    mins = load_minutes(a.win, assets)
    venue = load_venue(a.venue, a.code)
    use_secs = bool(a.secs_cache)
    secs = fetch_secs(windows, venue, a.secs_cache, verbose=True,
                      workers=max(1, a.workers), rate=a.rate,
                      max_windows=a.max_windows) if use_secs else {}
    if use_secs and not secs:
        print("  1с-кэш пуст и докачка не удалась -> режим по минутам", file=sys.stderr)
        use_secs = False
    ent, n_cov = build_entries(windows, mins, secs, venue, cps, a.fee_cents, use_secs)
    report(ent, len(windows), n_cov, a, use_secs, a.split_asset)


if __name__ == "__main__":
    main()
