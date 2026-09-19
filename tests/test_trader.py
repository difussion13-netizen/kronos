#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_trader — оффлайн-проверка M9 v0: decide()=формула clobmm, σ-фолбэк,
grid-филлы и P&L-переходы инвентаря, dry_submit-журнал, replay-пайплайн."""
import gzip
import json
import math
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "trader"))
import kronotrade as KT                                    # noqa: E402


def approx(a, b, eps=1e-9):
    assert abs(a - b) < eps, (a, b)


def test_decide_parity():
    # независимое воспроизведение формулы clobmm на фиксированных входах
    bench, spot, sigma, tau, step, k = 100_000.0, 100_010.0, 8.0, 240.0, 120.0, 2.5
    x_ref = (spot / bench - 1) * 1e4 / (sigma * math.sqrt(tau / 300.0))
    pa_ref = min(max(0.5 * (1 + math.erf(x_ref / math.sqrt(2))), 0.01), 0.99)
    h_ref = max(0.005, k * math.exp(-0.5 * x_ref ** 2) / math.sqrt(2 * math.pi)
                * math.sqrt(step / tau))
    d = KT.decide(bench, spot, sigma, 0.5, tau, step, k=k, anchor=True)
    approx(d["x"], x_ref); approx(d["center"], pa_ref); approx(d["h"], h_ref)
    approx(d["bid"], min(max(pa_ref - h_ref, 0.005), 0.99))
    approx(d["ask"], min(max(d["bid"] + 0.01, pa_ref + h_ref), 0.995))
    # нейтральный случай: spot==bench -> x=0, center=0.5
    d0 = KT.decide(100_000.0, 100_000.0, 8.0, 0.5, 240.0, 120.0, k=2.5)
    approx(d0["x"], 0.0); approx(d0["center"], 0.5)
    # clamp-границы
    d1 = KT.decide(100_000.0, 130_000.0, 2.0, 0.9, 240.0, 120.0, k=2.5)
    assert d1["pa"] <= 0.99 and d1["ask"] <= 0.995 and d1["bid"] >= 0.005
    # rebate-формула (clobmm.reb)
    approx(KT.reb(0.5), 0.2 * 0.0625 * 0.25 * 100.0)


def test_sigma_floor_and_window():
    mb = KT.MinuteBook()
    base = 100_000.0
    for i in range(130):                       # чередование ±10 б.п./минута
        base2 = base * math.exp((0.0010 if i % 2 else -0.0010))
        mb.add(i, round(base2, 4))
        base = base2
    sig = mb.sigma(129 * 60 + 30)
    approx(sig, 10.0 * math.sqrt(5.0), eps=0.6)  # std 10б.п. ×√5 (минус выборка)
    # короткий хвост -> 0 (недостаточно наблюдений)
    mb2 = KT.MinuteBook()
    for i in range(10):
        mb2.add(i, 100_000.0 + i)
    assert mb2.sigma(9 * 60) == 0.0


def test_engine_grid_flow():
    tmp = tempfile.mkdtemp()
    jp = os.path.join(tmp, "j.jsonl")
    eng = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), jp)
    start = 1_750_000_100 // 300 * 300
    m = KT.Market("btc", "5m", start, start + 300, "T1", "T2")
    eng.register(m)
    mb = KT.MinuteBook()
    eng.minutes["btc"] = mb
    for i in range(140):
        mb.add(start // 60 - 140 + i, 100_000.0)
    # mid == center => bid под средним, ask над: поставим книгу так, чтобы
    # cp-240 дал intent; затем mid на cp-120 провалился ниже bid-f -> fill BUY
    eng.book.best["T1"] = [0.50, 0.51]
    bench = mb.close(start)
    t1 = m.end - 240
    eng.on_second(t1)
    assert m.q_bid is not None, "котировка не выставлена на cp=240"
    bid, ask = m.q_bid, m.q_ask
    assert 0.005 <= bid < 0.99 and bid < ask <= 0.995
    mid2 = bid - 0.01                          # глубже f=0.005
    eng.book.best["T1"] = [mid2 - 0.005, mid2 + 0.005]
    eng.try_fills_grid(m, mid2)
    assert m.pos == 1 and abs(m.px - bid) < 1e-9
    # закрытие пары по ask: инвентарь +1, mid уходит вверх
    eng.q_fake = None
    m.q_bid, m.q_ask = None, (bid + 0.02)       # пере-выставили только ask
    mid3 = m.q_ask + 0.01
    eng.try_fills_grid(m, mid3)
    assert m.pos == 0
    stats = eng.stats
    assert stats["fills"] == 2 and stats["pairs"] == 1
    lines = [json.loads(l) for l in open(jp)]
    kinds = [l["ev"] for l in lines]
    assert kinds.count("dry_submit") == 2, kinds      # bid+ask на intent
    assert kinds.count("intent") == 1
    ds = [l for l in lines if l["ev"] == "dry_submit"]
    assert ds[0]["side"] == "BUY" and ds[1]["side"] == "SELL"
    assert abs(ds[0]["price"] - round(bid, 2)) < 1e-9
    assert ds[0]["size"] >= 5


def test_resolve_single():
    eng = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), None)
    start = 1_750_000_100 // 300 * 300
    m = KT.Market("btc", "5m", start, start + 300, "U1", "U2")
    eng.register(m)
    mb = KT.MinuteBook(); eng.minutes["btc"] = mb
    for i in range(130):
        mb.add(start // 60 - 130 + i, 100_000.0)
    for i in range(1, 4):                        # рост до end: fin>=bench -> y=1
        mb.add(start // 60 + i + 1, 100_100.0)
    m.pos, m.px = 1, 0.40                        # держим YES дешевле резолва
    eng.resolve(m, m.end)
    g = (1 - 0.40) * 100.0 + KT.reb(0.40)
    assert abs(eng.stats["pnl"] - g) < 1e-6, (eng.stats["pnl"], g)


def test_markout_resolve_semantics():
    """Гейт M9 №2 = M2 сима: markout сингла мерится платёжом резолва, а не live-mid
    (у live-mid на эндшпиле книги нет -> молча терялись все наблюдения)."""
    jp = tempfile.mktemp(suffix=".jsonl")
    eng = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), jp)
    start = 1_750_000_700 // 300 * 300
    m = KT.Market("btc", "5m", start, start + 300, "M1", "M2")
    eng.register(m)
    mb = KT.MinuteBook(); eng.minutes["btc"] = mb
    for i in range(130):
        mb.add(start // 60 - 130 + i, 100_000.0)
    for i in range(1, 4):                        # финал НИЖЕ бенча -> y=0
        mb.add(start // 60 + i + 1, 99_900.0)
    m.pos, m.px = 1, 0.55                        # купили YES, он проиграл
    eng.resolve(m, m.end)
    assert m.outcome == 0, m.outcome
    ev = [json.loads(x) for x in open(jp)]
    mo = [e for e in ev if e["ev"] == "markout"]
    assert len(mo) == 1 and mo[0]["src"] == "resolve", mo
    assert abs(mo[0]["d_cents"] - (-55.0)) < 1e-6, mo[0]      # (0 - 0.55)*100
    assert eng.stats["mk_res"] == 1 and eng.stats["mk_t60"] == 0
    # NO-нога: держим NO (pos=-1) по 0.45, YES проиграл -> NO выиграл: +55¢
    jp2 = tempfile.mktemp(suffix=".jsonl")
    eng2 = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), jp2)
    m2 = KT.Market("btc", "5m", start, start + 300, "M3", "M4")
    eng2.register(m2); eng2.minutes["btc"] = mb
    m2.pos, m2.px = -1, 0.45
    eng2.resolve(m2, m2.end)
    mo2 = [json.loads(x) for x in open(jp2) if '"markout"' in x]
    assert abs(mo2[0]["d_cents"] - 55.0) < 1e-6 and mo2[0]["lev"] == "ask", mo2


def test_bench_minute_parity():
    """Минута, против которой КВИТИРУЕМ == минута, против которой РЕЗОЛВИМСЯ.

    Регрессия на порт live-трейдера: checkpoint() брал bench = close(start-60),
    а resolve()/clobmm:159 — close(start) (закрытие минуты, СОДЕРЖАЩЕЙ start).
    По фактическим платёжам площадки это стоило +17.92 +- 3.96 cents/100 shares
    систематического «мы выиграли» (markcalc, блок J): вход, отобранный по
    смещённой метрике, ошибается односторонне.
    """
    jp = tempfile.mktemp(suffix=".jsonl")
    eng = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), jp)
    start = 1_750_000_400 // 300 * 300
    m = KT.Market("btc", "5m", start, start + 300, "Q1", "Q2")
    eng.register(m)
    mb = KT.MinuteBook(); eng.minutes["btc"] = mb
    for i in range(129):
        mb.add(start // 60 - 129 + i, 100_000.0)   # всё ДО границы
    mb.add(start // 60, 100_500.0)                  # close(start)      = 100500
    for i in range(1, 5):
        mb.add(start // 60 + i, 100_400.0)          # внутри окна
    assert mb.close(start - 60) == 100_000.0 and mb.close(start) == 100_500.0
    eng.book.best["Q1"] = [0.50, 0.51]
    eng.on_second(m.end - 240)
    it = [json.loads(l) for l in open(jp) if '"intent"' in l]
    assert it, "intent не записан (checkpoint не дошёл до decide)"
    assert it[0]["bench"] == 100_500.0, (it[0]["bench"], "bench обязан быть close(start)")
    # и та же минута решает исход: fin=100400 < bench=100500 -> Down,
    # тогда как на старой минуте (100000) было бы Up
    m.pos, m.px = 1, 0.20
    eng.resolve(m, m.end)
    assert m.outcome == 0, m.outcome
    assert abs(mb.close(m.end - 1) - 100_400.0) < 1e-9
    assert (1 if mb.close(m.end - 1) >= it[0]["bench"] else 0) == m.outcome
    assert eng.stats.get("cp_nobench", 0) == 0


def test_bench_age_telemetry():
    """Дыра в спот-фиде должна быть ВИДНА в журнале, а не молча съедена.

    MinuteBook.close(t) отдаёт последнюю минуту <= t — это правильно для живого
    фида и опасно при дыре: bench и fin берутся часовой давности, x≈0, а метка
    исхода становится случайной. Поэтому и intent, и markout@resolve пишут
    bench_age = сколько минут не хватило до границы окна; по нему markcalc
    выбрасывает рынок из САМОПРОВЕРКИ и из гейта, а не верит на слово.
    """
    jp = tempfile.mktemp(suffix=".jsonl")
    eng = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), jp)
    start = 1_750_000_700 // 300 * 300
    m = KT.Market("eth", "5m", start, start + 300, "R1", "R2")
    eng.register(m)
    mb = KT.MinuteBook(); eng.minutes["eth"] = mb
    for i in range(130):                             # непрерывно, РОВНО одной минуты нет
        mm = start // 60 - 129 + i
        if mm == start // 60:
            continue
        mb.add(mm, 4000.0 + 0.5 * (mm % 7))
    eng.book.best["R1"] = [0.50, 0.51]
    eng.on_second(m.end - 240)
    it = [json.loads(l) for l in open(jp) if '"intent"' in l]
    assert it, "intent не записан"
    assert it[0]["bench_age"] == 1, it[0].get("bench_age")   # minute start отсутствует
    assert it[0]["bench"] == mb.close(start - 60)    # вот она, reach-back на минуту
    m.pos, m.px = 1, 0.20
    eng.resolve(m, m.end)
    mo = [json.loads(l) for l in open(jp)
          if '"markout"' in l and '\"resolve\"' in l]
    assert mo and mo[0]["bench_age"] == 1, mo[:1]
    # и наоборот: если минута есть — возраст нулевой (контракт для markcalc)
    eng2 = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), None)
    mb2 = KT.MinuteBook()
    mb2.add(start // 60, 4005.0)
    assert mb2.item(start) == (start // 60, 4005.0) and mb2.close(start) == 4005.0
    assert mb2.item(start - 60) is None              # в прошлое не ходит


def test_markout_t60_counts_drops():
    """t60-ветка жива, когда книга есть, и ЧИТАЕМА (mk_drop), когда её нет."""
    jp = tempfile.mktemp(suffix=".jsonl")
    eng = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), jp)
    eng.book.apply("X1", {"type": "book",
                          "bids": [{"price": "0.40", "size": "10"},
                                   {"price": "0.30", "size": "10"}],
                          "asks": [{"price": "0.60", "size": "10"}]})
    eng.now = 1000.0
    import heapq as H
    H.heappush(eng.mq, (1060.0, "X1", "bid", 0.40, 0.5, "btc", 0))
    H.heappush(eng.mq, (1060.0, "Z9", "bid", 0.40, 0.5, "btc", 0))   # книги нет
    eng.now = 1061.0
    eng.flush_markouts(1061.0)
    ev = [json.loads(x) for x in open(jp) if '"markout"' in x]
    assert len(ev) == 1 and ev[0]["src"] == "t60", ev
    assert abs(ev[0]["d_cents"] - 10.0) < 1e-6, ev[0]      # mid 0.50 против 0.40
    assert eng.stats["mk_t60"] == 1 and eng.stats["mk_drop"] == 1, eng.stats


def test_replay_pipeline():
    tmp = tempfile.mkdtemp()
    dc = os.path.join(tmp, "clob", "20260910"); os.makedirs(dc)
    db = os.path.join(tmp, "binance", "20260910"); os.makedirs(db)
    start = 1_750_000_000 // 300 * 300
    t0 = start - 9600
    with gzip.open(os.path.join(db, "b_1.jsonl.gz"), "wt") as f:
        for i in range(166):
            ts = t0 + i * 60 + 30
            px = "100000.0" if ts < start + 120 else "100052.0"
            f.write(json.dumps({"t": ts * 10**9, "raw": {
                "e": "aggTrade", "s": "BTCUSDT", "T": ts * 1000,
                "p": px, "q": "1.0"}}) + "\n")

    def book(f, price, ts):
        f.write(json.dumps({"t": ts * 10**9, "raw": {
            "event_type": "book", "asset_id": "TK1",
            "bids": [{"price": str(round(price - 0.005, 4)), "size": "50"}],
            "asks": [{"price": str(round(price + 0.005, 4)), "size": "50"}]}}) + "\n")
    with gzip.open(os.path.join(dc, "c_1.jsonl.gz"), "wt") as f:
        book(f, 0.50, start + 60)     # cp=240: spot==bench -> широкие (clamp)
        book(f, 0.50, start + 180)    # cp=120: spot +5.2б.п. -> узкие ~0.99
        book(f, 0.95, start + 240)    # cp=60: mid<=bid-f -> fill bid
        book(f, 0.55, start + 300)    # тик разрешения
    eng = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), None)
    m = KT.Market("btc", "5m", start, start + 300, "TK1", "TK2")
    eng.register(m)
    n = KT.run_replay(eng, tmp, "20260910", fill_mode="grid")
    assert n > 150, n
    assert eng.stats["intents"] >= 3, eng.stats
    assert eng.stats["fills"] >= 1, eng.stats
    assert m.resolved and eng.stats["pnl"] > 0, (m.resolved, eng.stats)
    # fill-цена = выставленный bid cp=120
    fills = eng.stats["fills"]
    assert fills == 1, fills


def test_load_map_clobwin():
    import json, os, tempfile
    mp = {
        "111": {"slug": "btc-updown-5m-100", "asset": "btc", "code": "5m",
                "start": 100, "up": True},
        "222": {"slug": "btc-updown-5m-100", "asset": "btc", "code": "5m",
                "start": 100, "up": False},
        "333": {"slug": "eth-updown-15m-200", "asset": "eth", "code": "15m",
                "start": 200, "up": True},
        "334": {"slug": "eth-updown-15m-200", "asset": "eth", "code": "15m",
                "start": 200, "up": False},
        "444": {"slug": "btc-updown-4h-300", "asset": "btc", "code": "4h",
                "start": 300, "up": True},
        "445": {"slug": "btc-updown-4h-300", "asset": "btc", "code": "4h",
                "start": 300, "up": False},
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(mp, f); path = f.name
    eng = KT.Engine(dict(k=2.5, f=0.005, bet_usd=50.0, anchor=True), None)
    KT.load_map(eng, path, {"5m", "15m"})
    os.unlink(path)
    assert set(eng.markets) == {"111", "333"}, set(eng.markets)   # 4h вне --codes
    m = eng.markets["111"]
    assert (m.no, m.start, m.end) == ("222", 100, 400), (m.no, m.start, m.end)
    assert eng.markets["333"].end - eng.markets["333"].start == 900


def test_hour_filter_and_local_listing():
    import os, tempfile
    assert KT._names_hours("clob_20260917_134512.jsonl.gz") == 13
    assert KT._names_hours("junk.gz") == -1
    assert KT._hour_in(13, "12-14") and not KT._hour_in(15, "12-14")
    assert KT._hour_in(-1, None) and KT._hour_in(5, None)
    tmp = tempfile.mkdtemp()
    d = os.path.join(tmp, "clob", "20260910"); os.makedirs(d)
    for nm in ("clob_20260910_000858.jsonl.gz", "clob_20260910_235959.jsonl.gz"):
        open(os.path.join(d, nm), "wb").close()
    assert len(KT.stream_files_local(tmp, "clob", "20260910")) == 2
    got = KT.stream_files_local(tmp, "clob", "20260910", "00-00")
    assert [os.path.basename(x) for x in got] == ["clob_20260910_000858.jsonl.gz"]


def test_live_keys_initialized():
    """Каждый live["key"] внутри run_live обязан быть в инициализации dict.

    Зачем: 20.09 обработчик разрыва читал live["fails"], которого никто не заводил.
    KeyError из except = трейдер мёртв, journal молчал 40 минут, и видно это только
    тогда, когда канал уже упал. Функционально это не воспроизвести дёшево (нужен
    живой сокет, который падает), поэтому проверка статическая."""
    import ast
    path = os.path.join(os.path.dirname(__file__), "..", "trader", "kronotrade.py")
    tree = ast.parse(open(path, encoding="utf-8").read())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
              and n.name == "run_live")
    init, used = set(), set()
    for n in ast.walk(fn):
        if (isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
                and getattr(n.value.func, "id", "") == "dict"
                and any(isinstance(t, ast.Name) and t.id == "live" for t in n.targets)):
            init |= {k.arg for k in n.value.keywords if k.arg}
        if (isinstance(n, ast.Subscript) and isinstance(n.value, ast.Name)
                and n.value.id == "live" and isinstance(n.slice, ast.Constant)
                and isinstance(n.slice.value, str)):
            used.add(n.slice.value)
    assert used <= init, (f"live[...] без инициализации: {sorted(used - init)}; "
                          f"инициализировано: {sorted(init)}")


def main():
    test_decide_parity();            print("decide parity OK")
    test_hour_filter_and_local_listing(); print("hour filter OK")
    test_load_map_clobwin();         print("load_map clobwin format OK")
    test_sigma_floor_and_window();   print("sigma OK")
    test_engine_grid_flow();         print("engine grid flow OK")
    test_resolve_single();           print("resolve OK")
    test_markout_resolve_semantics(); print("markout@resolve (гейт M9 №2) OK")
    test_bench_minute_parity();        print("bench-minute parity OK")
    test_bench_age_telemetry();      print("bench_age telemetry OK")
    test_markout_t60_counts_drops();  print("markout t60 + mk_drop OK")
    test_replay_pipeline();          print("replay pipeline OK")
    test_live_keys_initialized();    print("live-счётчики инициализированы OK")
    print("test_trader: OK")


if __name__ == "__main__":
    main()
