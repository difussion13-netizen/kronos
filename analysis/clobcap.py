#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""clobcap.py — M6/M7: capacity из очереди + walk-forward fill-оракла.

Питается queue-csv от clobq.py (колонки day,code,cp,rank,price,s_at,traded,...).
Для каждого (code, x-лот) считает ОЖИДАЕМОЕ число лотов, которые реально
исполнятся сделками ЗА видимым объёмом уровня на ранге rank, на окно/сутки, и
переводит в $-оборот. Отдельно train/holdout сплит по суткам — проверка, что
оракл устойчив (гейт M7 «двойник имеет смысл»).

Fill-модель v1 (консервативная): стоим в хвосте очереди ранга r на цене
уровня; исполняемся на min(x, traded - s_at)_+ лотов. Off-book размещений и
price improvement нет — нижняя граница. P&L-наполнение — на стороне clobmm
(цена edge), здесь только ДОСТУПНЫЙ объём.

Использование:
  python3 clobcap.py --qdirs "/home/ssm-user/q1/*/queue_*.csv" \
      --train 20260910,20260911,20260912 --holdout 20260913,20260914
"""
import argparse
import csv
import glob
from collections import defaultdict

XS = (50, 100, 200, 400)          # лоты в заявке (1 лот = 1 share = price $)
CPS = (240, 120, 60, 30, 15, 5, 0)
RANKS = (0, 2)


def accumulate(paths, days_keep):
    """по (code,cp,rank,x): сумма fill-лотов и сумма $; плюс счётчики окон."""
    tot = defaultdict(lambda: [0.0, 0.0, 0])          # lots_sum, usd_sum, n
    per_day = defaultdict(lambda: defaultdict(float))  # day -> (code,x) -> usd
    ndays = set()
    for f in sorted(glob.glob(paths) if "*" in paths else [paths]):
        for row in csv.reader(open(f)):
            if row[0] == "day":
                continue
            day = row[0]
            if days_keep is not None and day not in days_keep:
                continue
            ndays.add(day)
            code, cp, rank = row[4], int(row[5]), int(row[7])
            price = float(row[8]); s_at = float(row[9]); traded = float(row[12])
            over = traded - s_at
            if code not in ("5m", "15m", "4h"):
                continue
            for x in XS:
                fill = min(x, over) if over > 0 else 0.0
                if fill <= 0:
                    continue
                key = (code, cp, rank, x)
                t = tot[key]
                t[0] += fill
                t[1] += fill * price
                t[2] += 1
                per_day[day][(code, x)] += fill * price
    return tot, per_day, max(len(ndays), 1)


def report(tag, tot, ndays, days_label):
    print(f"\n=== {tag} ({days_label}, {ndays} сут) ===")
    print(f"{'code':>4} {'cp':>4} {'rk':>2} | " +
          " | ".join(f"x={x:>4}: $/сут" for x in XS))
    for code in ("5m", "15m", "4h"):
        for cp in CPS:
            row = []
            for x in XS:
                t = tot.get((code, cp, 0, x))
                row.append(f"{(t[1]/ndays):>9.0f}" if t else f"{'0':>9}")
            print(f"{code:>4} {cp:>4} {'r0':>2} | " + " | ".join(row))
    print(f"{'':>11} суммарно доступный оборот $/сутки (все cp, ранг0):")
    for code in ("5m", "15m", "4h"):
        for x in XS:
            usd = sum(t[1] for k, t in tot.items()
                      if k[0] == code and k[2] == 0 and k[3] == x) / ndays
            print(f"  {code:>4} x={x:>4}: {usd:>10.0f}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--qdirs", required=True, help="glob по queue csv")
    p.add_argument("--train", default="")
    p.add_argument("--holdout", default="")
    a = p.parse_args()
    tr = set(x for x in a.train.split(",") if x) or None
    ho = set(x for x in a.holdout.split(",") if x) or None
    tot_tr, per_tr, n_tr = accumulate(a.qdirs, tr)
    tot_ho, per_ho, n_ho = accumulate(a.qdirs, ho)
    report("TRAIN", tot_tr, n_tr, a.train or "все")
    report("HOLDOUT", tot_ho, n_ho, a.holdout or "все")
    if tr and ho:
        print("\n=== walk-forward: $-оборот по дням (все cp, ранг0) ===")
        for tag, per in (("train", per_tr), ("holdout", per_ho)):
            for d in sorted(per):
                parts = " ".join(
                    f"{c}:x{x}={v:,.0f}" for (c, x), v in
                    sorted(per[d].items()) if x == 100)
                print(f"  {tag:7} {d}  {parts}")
        print("\nустойчивость (сумма |train-avg до holdout|/holdout по кодам, x=100):")
        for code in ("5m", "15m", "4h"):
            tr_s = sum(v for d in per_tr for (c, x), v in per_tr[d].items()
                       if c == code and x == 100) / max(n_tr, 1)
            ho_s = sum(v for d in per_ho for (c, x), v in per_ho[d].items()
                       if c == code and x == 100) / max(n_ho, 1)
            if ho_s > 0:
                r = abs(tr_s - ho_s) / ho_s
                print(f"  {code:>4}: train {tr_s:,.0f} $/сут vs holdout "
                      f"{ho_s:,.0f} -> откл {r:.0%}")
            else:
                print(f"  {code:>4}: holdout пусто — оценка невозможна")


if __name__ == "__main__":
    main()
