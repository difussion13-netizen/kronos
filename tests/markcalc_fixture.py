#!/usr/bin/env python3
"""Фикстура для analysis/markcalc.py: журнал «как будто с боевого прогона».

Два билда (старая конвенция bench=close(start-60) и новая close(start)), умышленная
дыра в клиновом кэше (рынок вне покрытия → nocov), рынок с bench_age=2 (дыра в
MinuteBook → исключение из гейтов) и дешёвый актив с квантованной телеметрией
(xrp: 2 знака после точки = 18 б.п. → «квант», а не «фид встал»).

Нужна потому, что у калькулятора нет юнит-тестов, а менять его приходится по живым
числам: без фикстуры правка «строгой минуты» прошла бы «успешно» и молча обнулила
бы все метки.

    python3 tests/markcalc_fixture.py /tmp/mkfix
    python3 analysis/markcalc.py --journal /tmp/mkfix/j.jsonl --cache /tmp/mkfix/kl \\
        --venue --venue-cache /tmp/mkfix/venue.jsonl --venue-sample 20
"""
import json
import os
import random
import sys
import time

DAY = 86400
S0 = 1_758_105_600 // 300 * 300      # старт «суток 1», кратно 5 мин
N = 6                                # рынков на сутки
PRICE = {"btc": 111_000.0, "eth": 4_300.0, "xrp": 2.85}
ASSETS = ("btc", "eth", "xrp")
K_HOLE = {"eth": S0 + 1 * 300}       # минута end-1 этого окна пропущена в кэше
AGE_HOLE = ("btc", S0 + DAY + 3 * 300)   # движок этого окна метил по минуте −2


def main(out):
    os.makedirs(os.path.join(out, "kl"), exist_ok=True)
    rnd = random.Random(7)
    px = dict(PRICE)
    first, last = S0 - 240, S0 + 2 * DAY
    kl = {a: [] for a in ASSETS}
    t = first
    while t < last:
        for a in ASSETS:
            px[a] *= (1 + rnd.gauss(0, 0.0002))
            o = px[a] * (1 + rnd.gauss(0, 0.00006))
            c = px[a]
            h = max(o, c) * (1 + abs(rnd.gauss(0, 0.00004)))
            lo = min(o, c) * (1 - abs(rnd.gauss(0, 0.00004)))
            kl[a].append([t * 1000, round(o, 2), round(h, 2),
                          round(lo, 2), round(c, 2)])
        t += 60
    full = {a: {r[0] // 60000: r[4] for r in kl[a]} for a in ASSETS}

    def close_at(a, ts):
        return full[a].get(int(ts) // 60)

    for a in ASSETS:
        with open(os.path.join(out, "kl", f"{a.upper()}USDT.jsonl"), "w") as f:
            for r in kl[a]:
                if a in K_HOLE and r[0] // 60000 == int(K_HOLE[a] + 240) // 60:
                    continue          # дыра ровно на минуте end-1 выбранного окна
                f.write(json.dumps(r) + "\n")

    jl, vl = [], []
    for day in (0, 1):
        for i in range(N):
            st = S0 + day * DAY + i * 300
            end = st + 300
            a = ASSETS[i % len(ASSETS)]
            code = "5m"
            # «движок» сутки 1 делит на close(start-60), сутки 2 — на close(start)
            bmin = st - 60 if day == 0 else st
            age_case = (a, st) == AGE_HOLE
            if age_case:
                bmin = st - 120                    # MinuteBook reach-back
            bench = close_at(a, bmin)
            fin = close_at(a, end - 1)
            if bench is None or fin is None:
                continue
            y_eng = 1 if fin >= bench else 0
            px_lvl = [0.30, 0.62, 0.95, 0.48, 0.15, 0.75][i]
            lev = "bid" if i % 2 == 0 else "ask"
            our = px_lvl if lev == "bid" else round(1 - px_lvl, 3)
            val = y_eng if lev == "bid" else 1 - y_eng
            qt = 2 if a == "xrp" else 8      # старый билд писал 2 знака после точки
            jl.append({"ts": st + 60, "ev": "intent", "asset": a, "start": st,
                       "code": code, "cp": 300, "bid": round(our - 0.01, 3),
                       "ask": round(our + 0.01, 3), "center": 0.5, "h": 0.004,
                       "x": 0.3, "bench": round(bench, qt), "spot": round(fin, qt),
                       "bench_age": 2 if age_case else 0})
            jl.append({"ts": st + 90, "ev": "fill", "asset": a, "start": st,
                       "code": code, "yes": 1 if lev == "bid" else 0,
                       "level": lev, "price": round(px_lvl, 3),
                       "shares": 100, "why": "dry"})
            jl.append({"ts": end + 5, "ev": "markout", "asset": a, "start": st,
                       "code": code, "yes": 1 if lev == "bid" else 0,
                       "lev": lev, "px": round(our, 3), "src": "resolve",
                       "bench_age": 2 if age_case else 0,
                       "d_cents": round((val - our) * 100.0, 2)})
            # площадка платит по СВОЕМУ правилу: tie/шум слегка расходится с нами
            ptb = close_at(a, st) or bench
            fp = fin * (1 + rnd.gauss(0, 0.00003))
            y_venue = 1 if fp >= ptb else 0
            vl.append({"asset": a, "start": st, "code": code, "y": y_venue,
                       "ptb": round(ptb, 2), "fp": round(fp, 2),
                       "closed": True, "slug": f"{a}-updown-{code}-{st}"})
    with open(os.path.join(out, "j.jsonl"), "w") as f:
        for r in jl:
            f.write(json.dumps(r) + "\n")
    with open(os.path.join(out, "venue.jsonl"), "w") as f:
        for r in vl:
            f.write(json.dumps(r) + "\n")
    # датасет-обманка для режима --venue-keys (окна, в которых мы НЕ стояли, тоже
    # должны становиться ключами сверки: на этом держится тейкер-исследование)
    with open(os.path.join(out, "windows_20250917.csv"), "w") as f:
        f.write("start,end,asset,code,n_book,n_pc,n_lt,vol_usd,b240,a240\n")
        for r in vl:
            f.write("%d,%d,%s,%s,1,1,1,100,.5,.5\n"
                    % (r["start"], r["start"] + 300, r["asset"], r["code"]))
    print(f"фикстура: {len(jl)} событий, {len(vl)} рынков в venue-кэше; "
          f"дыра в кэше клинов на {time.strftime('%m-%d %H:%M', time.gmtime(K_HOLE['eth'] + 240))} "
          f"(eth), reach-back MinuteBook на "
          f"{time.strftime('%m-%d %H:%M', time.gmtime(AGE_HOLE[1]))} (btc)")


HOWTO = """
Ожидания от прогонов на этой фикстуре (и что значит несоответствие):

  python3 analysis/markcalc.py --journal %(o)s/j.jsonl --cache %(o)s/kl \
      --venue --venue-cache %(o)s/venue.jsonl --venue-sample 20
    A) «макс расхождение = 0.00¢»          — реконструкция = движок, иначе формулы разъехались
    A) строки «ОТБРОШЕНО 1 …bench_age» и «вне покрытия клинов 1» — должны быть ровно по 1
    xrp: «мимо обеих» НЕ должно стать 100%%  — иначе квант телеметрии снова влез в метки
    САМОПРОВЕРКА «max расхождение … ( OK )» — end/bench/кэш совпадают с движком
    K)/M) считаются по n=11..12              — платёж площадки и точность знака

  python3 analysis/markcalc.py --venue-keys %(o)s --venue-cache %(o)s/venue.jsonl
    «с платежом (ptb+fp+y) 12 (100.0%%)» и «новых запросов не было» — сеть не трогается,
    каталог windows_*.csv разобран в ключи (режим для тейкер-исследования: платёж по
    ВСЕМ окнам, а не только по тем, где мы стояли)
"""


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/mkfix"
    main(out)
    print(HOWTO % {"o": out}, end="")
