#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""clobmm_sizes.py — офлайн-проверка трёх новых путей: размеры в freeze(), лоты/участие
в симе и ярлык «платёж площадки». Реальных данных не требует, сети не трогает.

    python3 tests/clobmm_sizes.py [outdir]     (по умолчанию /tmp/clobmmfix)

Почему это проверяется фикстурой, а не «на глаз по прогону»: права на ошибку в
единицах мы уже не имеем (рибейт был завышен в 100× ровно потому, что «P&L в $»
выглядело правдоподобно), а путь `inv_usd=0` обязан оставаться АРИФМЕТИЧЕСКИ тем же
прогоном, на котором висят гейты M1–M4.
"""
import csv
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "analysis"))
import clobmm                                          # noqa: E402
import clobwin                                         # noqa: E402

S = 1700000100 // 300 * 300
E = S + 300


def t_freeze():
    bids = {0.50: 100.0, 0.49: 50.0, 0.48: 10.0, 0.47: 900.0}
    asks = {0.51: 20.0, 0.52: 7.0, 0.53: 1000.0}
    b, a, bs, asz, d2b, d2a = clobwin.freeze(bids, asks, 0.02)
    assert (b, a, bs, asz) == (0.5, 0.51, 100.0, 20.0), (b, a, bs, asz)
    # границы окна ГЛУБИНЫ включительно: с bid-стороны 0.48 = ровно 2¢ вошло, 0.47 —
    # нет; с ask-стороны 0.53 = ровно 2¢ тоже вошло (асимметрия была бы багом)
    assert (d2b, d2a) == (160.0, 1027.0), (d2b, d2a)
    b, a, bs, asz, d2b, d2a = clobwin.freeze(bids, asks, 0.0)
    assert (d2b, d2a) == (100.0, 20.0), (d2b, d2a)      # окно 0 = только топ
    print("  freeze(): топ + глубина 2¢ и вырождение при 0¢ — OK")


HEAD = (["start", "end", "asset", "code", "n_book", "n_pc", "n_lt", "vol_usd"]
        + [x for c in clobmm.cp_list("5m")
           for x in (f"b{c}", f"a{c}", f"bs{c}", f"as{c}", f"d2b{c}", f"d2a{c}")])


def mkrow(st, vol):
    """Строка датасета: mid на всех чекпоинтах = 0.01, т.е. наш bid заведомо снят,
    a размеры топа/глубины выставлены так, чтобы любой фильтр участия ловил именно
    то, что от него ждут."""
    row = {"start": str(st), "end": str(st + 300), "asset": "btc", "code": "5m",
           "n_book": "10", "n_pc": "20", "n_lt": "5", "vol_usd": str(vol)}
    for c in clobmm.cp_list("5m"):
        row["b%d" % c] = "0.01"; row["a%d" % c] = "0.01"
        row["bs%d" % c] = "300"; row["as%d" % c] = "300"
        row["d2b%d" % c] = "900"; row["d2a%d" % c] = "900"
    return [row[k] for k in HEAD]


def build(out):
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "minutes_20260917.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["day", "minute", "asset", "close"])
        for i in range(-60, 12):
            w.writerow(["20260917", S + i * 60, "btc", round(100.0 + 0.1 * (i + 60), 4)])
    with open(os.path.join(out, "windows_20260917.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEAD)
        w.writerow(mkrow(S, 1e6))
        w.writerow(mkrow(S + 300, 1e6))
    with open(os.path.join(out, "venue.jsonl"), "w") as f:
        # площадка заплатила ПРОТИВОПОЛОЖНОЕ тому, что говорит наш клин (fin > bench)
        f.write(json.dumps({"asset": "btc", "start": S, "code": "5m", "y": 0,
                            "ptb": 100.30, "fp": 100.29}) + "\n")
        # второе окно: есть y, но нет ptb/fp -> платеж невосстановим, рынок пропускается
        f.write(json.dumps({"asset": "btc", "start": S + 300, "code": "5m", "y": 1,
                            "ptb": None, "fp": None}) + "\n")


def t_sim(out):
    assets, codes = {"btc"}, {"5m"}
    rows = clobmm.load_windows(out, assets, codes)
    assert len(rows) == 2, len(rows)
    per = clobmm.load_minutes(out)
    sig = clobmm.Sigma(per, {})
    f0 = 0.5 / 100.0

    base = clobmm.sim(rows, per, sig, True, 2.5, f0)
    # Механизм по фикстуре: на cp=240 x=0 -> h раздувается в 0.705 и заявка не
    # снимается; на cp=120 x велик -> center=0.99, h=0.005 -> bid=0.985, и его
    # снимают. Сингл доживает до резолва и по клиновому прокси ВЫИГРЫВАЕТ:
    # 1.5¢/акц запаса над ценой. Проверяем именно это, а не формулу (формула —
    # дословная копия близнеца, её регрессию ловит test_trader).
    assert base["singles"] == 2 and base["fills"] == 2 and base["pairs"] == 0, base
    assert 1.4 < base["mark_s_med"] < 1.6, base["mark_s_med"]
    assert base["pnl"] > 0 and base["thin"] == 0 and base["venue_n"] == 0, base
    # лоты: P&L ЛИНЕЕН по размеру заявки, метки (¢/акц) не тронуты
    a100 = clobmm.sim(rows, per, sig, True, 2.5, f0, inv_usd=100.0)
    a200 = clobmm.sim(rows, per, sig, True, 2.5, f0, inv_usd=200.0)
    assert abs(a200["pnl"] / a100["pnl"] - 2.0) < 1e-9, (a100["pnl"], a200["pnl"])
    assert abs(a100["mark_s_med"] - base["mark_s_med"]) < 1e-9
    # lots = inv/(цена·100): при цене 98.5¢ заявка $100 = 101.5 акц, а НЕ «100
    # акций» (акция стоит не доллар!). На $100 разница — полпроцента, тонет в
    # округлении pnl до 0.1¢, поэтому проверяем на $500: lots = 5.076, и P&L
    # обязан вырасти во столько же раз.
    a500 = clobmm.sim(rows, per, sig, True, 2.5, f0, inv_usd=500.0)
    lo, hi = base["pnl"] * 500.0 / 98.5 * 0.97, base["pnl"] * 500.0 / 98.5 * 1.03
    assert lo <= a500["pnl"] <= hi, (a500["pnl"], lo, hi)
    assert a500["mark_s_med"] == base["mark_s_med"]     # метки — по-прежнему ¢/акц
    # участие: заявка больше 5% оборота тонкого окна -> не выставляем
    thin = [dict(rows[0], vol_usd="1000.0")]
    t = clobmm.sim(thin, per, sig, True, 2.5, f0, inv_usd=100.0, max_part=0.05)
    assert t["thin"] == 1 and t["singles"] == 0, t
    t = clobmm.sim(thin, per, sig, True, 2.5, f0, inv_usd=10.0, max_part=0.05)
    assert t["thin"] == 0 and t["singles"] == 1, t
    # ярлык = платёж: y флипается, P&L становится отрицательным, второй рынок
    # (без платежа) НЕ размечается прокси, а пропускается
    ven = clobmm.load_venue(os.path.join(out, "venue.jsonl"), "5m")
    assert set(ven) == {("btc", S)}, ven
    v = clobmm.sim(rows, per, sig, True, 2.5, f0, venue=ven)
    assert v["venue_n"] == 1 and v["no_venue"] == 1 and v["singles"] == 1, v
    assert v["singles"] == 1 and v["mark_s_med"] < -98.0, v
    assert -99.5 < v["mark_s_med"] < -98.0, v["mark_s_med"]
    # интервалы держания (для профиля одновременного капитала)
    assert len(base["iv"]) == 2 and all(u > 0 for _a, _b, u in base["iv"]), base["iv"]
    print("  sim(): lots-линейность, совпадение при inv=0, max-part, платёжный ярлык, "
          "пропуск без платежа, iv — OK")
    print(f"    контрольные числа: P&L(lots=1, т.е. 100 акц)={base['pnl']:.4f}$, "
          f"P&L($100)={a100['pnl']:.4f}, P&L(платёж)={v['pnl']:.4f}")


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/clobmmfix"
    t_freeze()
    build(out)
    t_sim(out)
    print("clobmm_sizes: OK")


if __name__ == "__main__":
    main()
