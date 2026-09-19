#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Синтетический тест clobq.py (регрессия). Гонять: python3 tests/test_clobq.py
Покрывает: семантику price_change.side (BUY=уровень bids, SELL=уровень asks),
взятие top-5 на чекпоинтах, running min/end, traded-накопление по last_trade.
"""
import csv
import datetime as dt
import gzip
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CLOBQ = os.path.join(HERE, "..", "analysis", "clobq.py")


def main():
    d = "/tmp/qtest/20260910"
    os.makedirs(d, exist_ok=True)
    start = int(dt.datetime(2026, 9, 10, 10, 0, tzinfo=dt.timezone.utc).timestamp())
    end = start + 300
    tok = "111"

    def E(t, **kw):
        return json.dumps({"t": int(t * 1e9),
                           "raw": {"timestamp": str(int(t)), **kw}},
                          separators=(",", ":"))

    def B(p, s):
        return {"price": str(p), "size": str(s)}

    book = lambda t, b, a: E(t, event_type="book", asset_id=tok, bids=b, asks=a)
    pc = lambda t, ch: E(t, event_type="price_change", asset_id=tok, changes=ch)
    lt = lambda t, p, s, side: E(t, event_type="last_trade_price", asset_id=tok,
                                 price=str(p), size=str(s), side=side)
    lines = [book(start, [B(0.40, 100), B(0.39, 50)], [B(0.41, 80), B(0.42, 30)])]
    for t in (start + 30, start + 60):
        lines.append(book(t, [B(0.40, 100), B(0.39, 50)], [B(0.41, 80), B(0.42, 30)]))
    # правки УРОВНЕЙ: side=BUY бидит bids, side=SELL — asks
    lines.append(pc(end - 200, [{"side": "BUY", "price": 0.4, "size": 60}]))  # числа без кавычек
    lines.append(lt(end - 150, 0.40, 40, "SELL"))            # тейк-селл ест bid 0.40
    lines.append(lt(end - 120, 0.41, 100, "BUY"))            # тейк-бай ест ask 0.41
    lines.append(pc(end - 100, [{"side": "SELL", "price": 0.41, "size": 0}]))
    with gzip.open(d + "/part1.jsonl.gz", "wt") as f:
        f.write("\n".join(lines))
    mp = {tok: {"slug": f"btc-updown-5m-{start}", "asset": "btc",
                "code": "5m", "start": start, "up": True}}
    json.dump(mp, open("/tmp/qtest/tokens_map.json", "w"))

    r = subprocess.run([sys.executable, CLOBQ, "--days", "20260910",
                        "--outdir", "/tmp/qtest/out",
                        "--map-local", "/tmp/qtest/tokens_map.json",
                        "--local-dir", d], capture_output=True, text=True)
    if r.returncode:
        print(r.stdout, r.stderr, file=sys.stderr)
        raise SystemExit("clobq упал")
    rows = list(csv.DictReader(open("/tmp/qtest/out/queue_20260910.csv")))
    q = lambda cp, side, pr: [x for x in rows
                              if x["cp"] == cp and x["side"] == side
                              and x["price"] == pr][0]
    r1 = q("240", "bid", "0.4")
    assert float(r1["s_at"]) == 100 and float(r1["s_min"]) == 60 \
        and float(r1["s_end"]) == 60 and float(r1["traded"]) == 40 \
        and abs(float(r1["consumed_frac"]) - 0.4) < 1e-9, r1
    r2 = q("240", "ask", "0.41")
    assert float(r2["traded"]) == 100 and float(r2["s_end"]) == 0 \
        and float(r2["s_min"]) == 0, r2
    # 2 cp-снапшота (240/120) × до 5 уровней: поздние cp не берутся — события
    # кончились раньше; минимум: все 4 уровня на cp=240 + 4 на cp=120
    assert len(rows) >= 8, len(rows)
    assert q("120", "bid", "0.39")["rank"] == "1", q("120", "bid", "0.39")
    print("test_clobq: OK (строк:", len(rows), ")")


if __name__ == "__main__":
    main()
