#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""kronotrade.py — M9 v0: трейдер = clobmm в живом цикле (DRY RUN).

Железное правило (docs/m9-trader-design.md): формулы решения ДОСЛОВНО из
analysis/clobmm.py (sim()). Любое расхождение — дефект. Ни ключей, ни отправки:
dry_submit пишет порядок в journal.

Режимы:
  replay: python3 kronotrade.py --replay DIR [--map tokens_map.json] --day YYYYMMDD
          DIR = локальная копия kronolog/{clob,binance}/YYYYMMDD/*.jsonl.gz
  live  : python3 kronotrade.py --live [--assets btc,eth,sol,xrp]
          (websockets из venv логгера; подписки дописываются с operation=subscribe)

Заполнители датасета в replay: минуты спота = aggTrade binance; книга = clob
book/price_change; окна = tokens_map.json {token_id: {asset,code,start,end}}.
"""
import argparse
import collections
import bisect
import glob
import gzip
import heapq
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request

SQ2PI = math.sqrt(2.0 * math.pi)
REB = 0.2 * 0.0625          # rebate-доля (×0.0625 taker fee), как в clobmm
TICK = 0.01


def phi(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def pdf(x):
    return math.exp(-0.5 * x * x) / SQ2PI


def reb(p):
    return REB * min(max(p, 0.0), 1.0) * (1.0 - min(max(p, 0.0), 1.0)) * 100.0


CODE_S = {"5m": 300, "15m": 900, "4h": 14400}


def _names_hours(name):
    # "clob_20260917_134512.jsonl.gz" -> 13
    i = name.rfind("_")
    h = name[i + 1:i + 3] if i >= 0 else ""
    return int(h) if h.isdigit() else -1


def _hour_in(h, spec):
    if not spec or h < 0:
        return True
    a, b = spec.split("-")
    return int(a) <= h <= int(b)


def stream_files_local(dirs, stream, day, hours=None):
    for pat in (os.path.join(dirs, stream, day, "*.jsonl.gz"),
                os.path.join(dirs, stream, "*.jsonl.gz")):
        g = sorted(glob.glob(pat))
        if g:
            return [x for x in g if _hour_in(_names_hours(os.path.basename(x)), hours)]
    return []


def stream_files_s3(bucket, prefix, stream, day, hours=None):
    r = subprocess.run(f"aws s3 ls s3://{bucket}/{prefix}/{stream}/{day}/",
                       capture_output=True, text=True, shell=True)
    if r.returncode != 0:
        print(f"s3 ls {stream}/{day}: rc={r.returncode} "
              f"{(r.stderr or '').strip()[:200]}", file=sys.stderr)
    out = []
    for ln in (r.stdout or "").splitlines():
        name = ln.split()[-1] if ln.split() else ""
        if name.endswith(".jsonl.gz") and _hour_in(_names_hours(name), hours):
            out.append(f"aws s3 cp s3://{bucket}/{prefix}/{stream}/{day}/{name} - | zcat")
    return sorted(out)


def _lines(src):
    """(proc|fh_closeable, line_iter) — локальный .gz или shell-пайп из S3."""
    if src.endswith(".gz"):
        return None, gzip.open(src, "rb")
    pr = subprocess.Popen(src, shell=True, stdout=subprocess.PIPE)
    return pr, pr.stdout


def cp_list(code):
    if code == "5m":
        return [240, 120, 60, 30, 15, 5, 0]
    if code == "15m":
        return [600, 240, 120, 60, 30, 15, 5, 0]
    return [1800, 600, 240, 120, 60, 30, 15, 5, 0]


def decide(bench, spot, sigma, mid, tau, step, k, anchor=True):
    """Точная кодия clobmm.sim: строки center/h/bid/ask/fill-критерии."""
    d_bps = (spot / bench - 1.0) * 1e4
    x = d_bps / (sigma * math.sqrt(tau / 300.0))
    pa = min(max(phi(x), 0.01), 0.99)
    center = pa if anchor else mid
    h = max(0.005, k * pdf(x) * math.sqrt(step / tau))
    bid = min(max(center - h, 0.005), 0.99)
    ask = min(max(bid + TICK, center + h), 0.995)
    return dict(bid=bid, ask=ask, x=x, pa=pa, center=center, h=h)


class MinuteBook:
    """close спота по минутам; доступ строго до t−60с (информационный урок E3)."""

    def __init__(self):
        self.ts, self.cl = [], []

    def add(self, minute, close):
        if self.ts and self.ts[-1] == minute:
            self.cl[-1] = close
        elif not self.ts or minute > self.ts[-1]:
            self.ts.append(minute); self.cl.append(close)

    def item(self, t):                       # (минута, close) последней минуты <= t
        i = bisect.bisect_right(self.ts, t // 60) - 1
        return (self.ts[i], self.cl[i]) if i >= 0 else None

    def close(self, t):                      # последняя минута <= t
        it = self.item(t)
        return it[1] if it else None

    def sigma(self, t):                       # std log-ret за 120 мин ×√5, floor 2
        j = bisect.bisect_right(self.ts, t // 60) - 1
        lo = max(0, j - 120)
        rs = []
        for i in range(lo + 1, j + 1):
            if self.ts[i] - self.ts[i - 1] <= 2 and self.cl[i - 1] > 0:
                rs.append(math.log(self.cl[i] / self.cl[i - 1]) * 1e4)
        if len(rs) < 30:
            return 0.0
        m = sum(rs) / len(rs)
        var = sum((v - m) ** 2 for v in rs) / len(rs)
        return max(2.0, math.sqrt(var) * math.sqrt(5.0))


class Book:
    """Полный ладдер на токен: book-снапшот = замена, price_change = дельты.
    mid строится по реальным уровням, а не 'угадыванию из best' — меньше
    cp_nomid-пропусков между снапшотами, решения fresher."""
    def __init__(self):
        self.best = {}                        # token -> [bid, ask]
        self.lad = {}                         # token -> ({bid: sz}, {ask: sz})

    def drop(self, tokens):
        """Забыть состояние токенов мёртвого окна (иначе ладдер растёт весь день)."""
        for t in tokens:
            self.best.pop(t, None)
            self.lad.pop(t, None)

    def apply(self, token, ev):
        et = ev.get("event_type") or ev.get("type") or ""
        if et == "book":
            bids = {}
            asks = {}
            for x in (ev.get("bids") or ev.get("buys") or []):
                try:
                    bids[float(x["price"])] = float(x["size"])
                except Exception:
                    pass
            for x in (ev.get("asks") or ev.get("sells") or []):
                try:
                    asks[float(x["price"])] = float(x["size"])
                except Exception:
                    pass
            self.lad[token] = (bids, asks)
            self.best[token] = [max(bids) if bids else 0.0,
                                min(asks) if asks else 1.0]
        elif et == "price_change":
            lad = self.lad.get(token)
            if lad is None:
                return                        # снапшота ещё нет — по дельтам не гадаем
            bids, asks = lad
            for ch in ev.get("price_changes") or ev.get("changes") or []:
                try:
                    pz = float(ch.get("price", 0))
                    sz = float(ch.get("size", 0))
                except Exception:
                    continue
                side = str(ch.get("side", "")).upper()
                d = bids if side == "BUY" else (asks if side == "SELL" else None)
                if d is None:
                    continue
                if sz > 0:
                    d[pz] = sz
                else:
                    d.pop(pz, None)
            self.best[token] = [max(bids) if bids else 0.0,
                                min(asks) if asks else 1.0]

    def mid(self, token):
        b = self.best.get(token)
        if not b or b[0] <= 0 or b[1] >= 1 or b[0] > b[1]:
            return None
        return (b[0] + b[1]) / 2.0


class Market:
    __slots__ = ("asset", "code", "start", "end", "yes", "no", "pos", "px",
                 "q_bid", "q_ask", "q_open_ts", "bench_seen", "resolved",
                 "outcome")

    def __init__(self, asset, code, start, end, yes, no):
        self.asset, self.code = asset, code
        self.start, self.end = start, end
        self.yes, self.no = yes, no
        self.pos, self.px = 0, 0.0
        self.q_bid = self.q_ask = None
        self.q_open_ts = 0
        self.bench_seen = False
        self.resolved = False
        self.outcome = None                    # 1 = YES выиграл (для markout)


class Engine:
    def __init__(self, cfg, journal_path):
        self.cfg = cfg
        self.minutes = {}                     # asset -> MinuteBook
        self.book = Book()
        self.markets = {}                     # yes_token -> Market
        self.heap = []
        self.jf = open(journal_path, "a") if journal_path else None
        self.now = 0.0
        self.mq = []        # (t+60, token, lev, px, mid0, asset, start)
        self.stats = dict(intents=0, fills=0, pairs=0, singles=0, quote_s=0.0,
                          pnl=0.0, days=set(), book=0, cp_ok=0, cp_nobench=0,
                           cp_nomid=0, cp_nosig=0, cp_stale=0, cp_nomin=0,
                           mk_res=0, mk_t60=0, mk_drop=0)

    # ---- живые окна и уборка -------------------------------------------------
    # Подписка = то, что ещё можно котировать, а НЕ «всё, когда-либо найденное».
    # 19.09 на боксе это дало самоподдерживающийся шторм: markets рос до 92
    # (ids=184), после каждого 1013 reconnect слал 14 чанков подписок ДО запуска
    # читателя, т.е. 40 секунд никто не читал — серверный send buffer переливался,
    # прилетало 1013 «slow consumer» и 1011 «keepalive ping timeout», круг замыкался.
    # Лечится двумя вещами сразу: множество подписки = живые окна, и читатель
    # запускается ДО рассылки подписок (см. live-loop).
    def live_ids(self, now, ahead=900, grace=300):
        out = set()
        for m in self.markets.values():
            if m.pos or (m.start - ahead <= now <= m.end + grace):
                out.add(m.yes); out.add(m.no)
        return out

    def n_live(self, now, ahead=900, grace=300):
        return sum(1 for m in self.markets.values()
                   if m.pos or (m.start - ahead <= now <= m.end + grace))

    def prune(self, now, grace=300):
        """Убрать окна, которые уже нельзя ни котировать, ни разрешить.

        Позиция != 0 — не трогаем НИКОГДА: markout/resolve обязаны достаться
        каждому филлу, иначе гейт №2 молча теряет наблюдения.
        """
        dead = [tok for tok, m in self.markets.items()
                if not m.pos and (m.resolved or now > m.end + grace)]
        for tok in dead:
            m = self.markets.pop(tok)
            self.book.drop([m.yes, m.no])
        return len(dead)

    # ---- события ----
    def minute(self, asset, ts, close):
        self.minutes.setdefault(asset, MinuteBook()).add(int(ts // 60), close)

    def book_ev(self, token, ev):
        self.book.apply(token, ev)
        self.stats["book"] += 1

    # ---- markout(60s) после fill: adverse selection в тех же центах, что M5 ----
    def flush_markouts(self, now):
        while self.mq and self.mq[0][0] <= now:
            t1, tok, lev, px, mid0, asset, start = heapq.heappop(self.mq)
            mid1 = self.book.mid(tok)
            if mid1 is None:
                self.stats["mk_drop"] += 1          # endgame: книги уже нет
                continue
            d = (mid1 - px) if lev == "bid" else (px - mid1)
            self.stats["mk_t60"] += 1
            self.j({"ts": int(t1), "ev": "markout", "asset": asset,
                    "start": start, "lev": lev, "px": round(px, 3), "src": "t60",
                    "d_cents": round(d * 100.0, 2)})

    def register(self, m: Market):
        self.markets[m.yes] = m
        for c in [x for x in cp_list(m.code) if x >= 60]:
            self.heap.append((m.end - c, m.yes, c))
        self.heap.append((m.end, m.yes, None))       # resolve
        heapq.heapify(self.heap)

    def on_second(self, now):
        self.now = now
        self.flush_markouts(now)
        while self.heap and self.heap[0][0] <= now:
            t1, tok, c = heapq.heappop(self.heap)
            m = self.markets.get(tok)
            if m is None or m.resolved:
                continue
            if now - t1 > self.cfg.get("stale", 2):
                self.stats["cp_stale"] += 1
                continue                              # просроченное — мимо
            if c is None:
                self.resolve(m, t1)
            else:
                self.checkpoint(m, cp_list(m.code), c, t1)

    def checkpoint(self, m, cps, c, t1):
        mb = self.minutes.get(m.asset)
        if not mb:
            self.stats["cp_nomin"] += 1
            return
        # Тот же бар, что и у метки исхода: clobmm:159 берёт mget(per, asset, start),
        # т.е. ЗАКРЫТИЕ минуты, СОДЕРЖАЩЕЙ start (бары ключуются int(ts//60) от
        # сделки, так что это не «цена до start»). resolve() ниже метит ровно по
        # mb.close(m.start) — минута решения и минута разрешения обязаны быть
        # одной: ошибка в величине, на которую смотрит вход, не симметрична
        # (она отбирает сделки), поэтому live-решение против close(start-60)
        # дало +17.92 +- 3.96 cents/100 shares систематического самообмана по
        # фактическим платёжам площадки (analysis/markcalc.py, блок J, n=106).
        it0 = mb.item(m.start)
        bench = it0[1] if it0 else None
        spot = mb.close(t1 - 60) or bench
        if bench is None or spot is None:
            self.stats["cp_nobench"] += 1
            return
        bage = int(m.start // 60 - it0[0])
        i = cps.index(c)
        c2 = cps[i + 1] if i + 1 < len(cps) else None
        tau = float(max(10, c))
        step = float(max(15, min((c - (c2 if c2 is not None else 0)), int(tau))))
        mid = self.book.mid(m.yes)
        if mid is None:
            self.stats["cp_nomid"] += 1
            return
        sig = mb.sigma(t1 - 60)
        if sig <= 0:
            self.stats["cp_nosig"] += 1
            return
        self.stats["cp_ok"] += 1
        d = decide(bench, spot, sig, mid, tau, step,
                   k=self.cfg["k"], anchor=self.cfg["anchor"])
        self.cancel(m, t1)
        self.place(m, d, t1)
        # bench/spot в журнал — телеметрия: без неё спор «против какой минуты мы
        # kvотируем» решался бы только офлайн-реконструкцией клинов
        self.j({"ts": t1, "ev": "intent", "asset": m.asset, "code": m.code,
                "start": m.start, "cp": c, "bench": self.q(bench),
                "spot": self.q(spot), "bench_age": bage,
                **{x: round(d[x], 4) for x in
                   ("bid", "ask", "center", "h", "x")}})

    # ---- ордера (dry) ----
    def place(self, m, d, t1):
        shares = int(self.cfg["bet_usd"] / max(d["bid"], 0.05))
        m.q_bid, m.q_ask = d["bid"], d["ask"]
        m.q_open_ts = t1
        for side, tok, px in (("BUY", m.yes, d["bid"]),
                             ("SELL", m.yes, d["ask"])):
            self.dry_submit(tok, side, px, max(shares, 5), t1)
        self.stats["intents"] += 1

    def cancel(self, m, t1):
        if m.q_bid is not None and m.q_open_ts:
            self.stats["quote_s"] += max(0.0, min(t1 - m.q_open_ts, 3600.0))
            self.j({"ts": t1, "ev": "quote_close", "asset": m.asset,
                    "start": m.start, "sec": t1 - m.q_open_ts})
        m.q_bid = m.q_ask = None; m.q_open_ts = 0

    def dry_submit(self, token, side, price, size, ts):
        # M9.0:形状 CLOB-ордера, без подписи/отправки. Порядок полей =
        # py-clob-client (salt, maker, signer, taker, tokenId, makerAmount,
        # takerAmount, side, feeRateBps, nonce, expiration).
        self.j({"ts": ts, "ev": "dry_submit", "token": token, "side": side,
                "price": round(price, 2), "size": size})

    # ---- исполнение: grid = копия clobmm (след. cp), live = непрерывно ----
    def try_fills_grid(self, m, mid2):
        f = self.cfg["f"]
        if m.q_bid is not None and m.pos <= 0 and mid2 <= m.q_bid - f:
            self.fill(m, "bid", m.q_bid, mid2)
        if m.q_ask is not None and m.pos >= 0 and mid2 >= m.q_ask + f:
            self.fill(m, "ask", m.q_ask, mid2)

    def try_fills_live(self, m, now):
        f = self.cfg["f"]
        mid = self.book.mid(m.yes)
        if mid is None or m.q_bid is None:
            return
        if m.pos <= 0 and mid <= m.q_bid - f:
            self.fill(m, "bid", m.q_bid, mid)
        elif m.pos >= 0 and mid >= m.q_ask + f:
            self.fill(m, "ask", m.q_ask, mid)

    def fill(self, m, lev, px_lvl, mid_now):
        self.stats["fills"] += 1
        slip = (mid_now - px_lvl) if lev == "bid" else (px_lvl - mid_now)
        self.j({"ts": int(time.time()), "ev": "fill", "asset": m.asset,
                "start": m.start, "level": lev, "price": round(px_lvl, 4),
                "mid": round(mid_now, 4), "slip_bps": round(slip * 1e4, 1)})
        heapq.heappush(self.mq, (self.now + 60.0, m.yes, lev, px_lvl,
                                 mid_now, m.asset, m.start))
        f = self.cfg["f"]
        if lev == "bid":                                  # купили YES
            if m.pos == -1:
                g = (1.0 - px_lvl - m.px) * 100.0
                self.pnl(m, g + reb(px_lvl)); m.pos = 0
                self.stats["pairs"] += 1
            elif m.pos == 0:
                m.pos, m.px = 1, px_lvl
        else:                                             # спросили наш ASK
            if m.pos == 1:
                g = (px_lvl - m.px) * 100.0
                self.pnl(m, g + reb(px_lvl)); m.pos = 0
                self.stats["pairs"] += 1
            elif m.pos == 0:
                m.pos, m.px = -1, 1.0 - px_lvl
        # после fill уровень снимается (симулятор держит до след. cp — паритет
        # grid-режима обеспечивает try_fills_grid, live — пере-выставление)
        m.q_bid = m.q_ask = None

    def resolve(self, m, now):
        m.resolved = True
        self.cancel(m, now)
        mb = self.minutes.get(m.asset)
        it = mb.item(m.start) if mb else None       #MinuteBook.close() отдаёт
        bench = it[1] if it else None               # ПОСЛЕДНЮЮ минуту <= t, т.е. при
        fin = mb.close(m.end - 1) if mb else None   # дыре в фиде — часы назад
        if bench is None or fin is None:
            return
        bage = int(m.start // 60 - it[0])            # сколько минут не хватило фиду
        y = 1 if fin >= bench else 0
        m.outcome = y
        if m.pos != 0:
            val = y if m.pos == 1 else 1 - y
            # гейт-метрика = M2 сима: markout сингла по платёжу резолва, без рибейта
            self.stats["mk_res"] += 1
            self.j({"ts": int(now), "ev": "markout", "asset": m.asset,
                    "start": m.start, "lev": "bid" if m.pos == 1 else "ask",
                    "px": round(m.px, 3), "src": "resolve", "bench_age": bage,
                    "d_cents": round((val - m.px) * 100.0, 2)})
            self.pnl(m, (val - m.px) * 100.0 + reb(m.px))
            self.stats["singles"] += 1

    def pnl(self, m, x):
        self.stats["pnl"] += x
        self.stats["days"].add(m.start // 86400)

    @staticmethod
    def q(x):
        """8 ЗНАЧАЩИХ цифр вместо «2 знака после точки». Для XRP по цене $2.8
        цент = 36 б.п., и офлайн-сверка «какую минуту делил движок» превращается
        в монетку (прогон 19.09: xrp 23 б.п. «мимо обеих минут» ровно поэтому).
        8 значащих = 0.001 б.п. при любом уровне цены."""
        try:
            return float(f"{x:.8g}")
        except Exception:
            return None

    def j(self, d):
        if self.jf:
            self.jf.write(json.dumps(d, separators=(",", ":")) + "\n")
            self.jf.flush()


# ---------------- replay ----------------
RX_CLOB_ASSET = re.compile(rb'"asset_id":\s*"?(\d+)"?')


def run_replay_srcs(engine, bin_srcs, clob_srcs, fill_mode="grid"):
    n = 0
    for path in bin_srcs:                            # pass 1: минуты спота
        try:
            pr, fh = _lines(path)
        except Exception:
            continue
        with fh:
            for raw in fh:
                n += 1
                try:
                    env = json.loads(raw); msg = env.get("raw") or {}
                except Exception:
                    continue
                if isinstance(msg, str):
                    try:
                        msg = json.loads(msg)
                    except Exception:
                        continue
                if isinstance(msg, dict):
                    dat = msg.get("data") or msg          # combined-stream обёртка
                    if dat.get("e") == "aggTrade":
                        asset = str(dat.get("s", "")).lower().replace("usdt", "")
                        if asset in ("btc", "eth", "sol", "xrp"):
                            engine.minute(asset, int(float(dat["T"]) / 1000),
                                          float(dat["p"]))
        if pr is not None:
            try: pr.stdout.close()
            except Exception: pass
            pr.wait()
    for path in clob_srcs:                            # pass 2: книга+решения
        try:
            pr, fh = _lines(path)
        except Exception:
            continue
        with fh:
            for raw in fh:
                n += 1
                try:
                    env = json.loads(raw); t = int(env.get("t", 0)) // 10**9
                    msg = env.get("raw") or {}
                except Exception:
                    continue
                if isinstance(msg, str):
                    try:
                        msg = json.loads(msg)
                    except Exception:
                        continue
                if not isinstance(msg, dict):
                    continue
                et = msg.get("event_type") or msg.get("type") or ""
                if et in ("book", "price_change"):
                    toks = [msg.get("asset_id")] if msg.get("asset_id") else \
                        msg.get("assets_ids") or []
                    for tk in toks:
                        tok = str(tk)
                        if tok in engine.markets:
                            engine.book_ev(tok, msg)
                            if fill_mode == "continuous":
                                engine.try_fills_live(engine.markets[tok], t)
                    if fill_mode == "grid":
                        # семантика clobmm: висящая котировка проверяется по
                        # mid текущего момента (в симе — mid следующего cp)
                        for mm in engine.markets.values():
                            if mm.q_bid is not None:
                                mv = engine.book.mid(mm.yes)
                                if mv is not None:
                                    engine.try_fills_grid(mm, mv)
                if t:
                    engine.on_second(t)
                if n % 2000000 == 0:
                    print(f"  replay {os.path.basename(path.split(' ')[0])}: "
                          f"{n} строк, books={engine.stats['book']} "
                          f"cp(ok/nob/nomid/nosig/stale/nomin)="
                          f"{engine.stats['cp_ok']}/{engine.stats['cp_nobench']}/"
                          f"{engine.stats['cp_nomid']}/{engine.stats['cp_nosig']}/"
                          f"{engine.stats['cp_stale']}/{engine.stats['cp_nomin']} "
                          f"min={len(engine.minutes)}", file=sys.stderr)
        if pr is not None:
            try: pr.stdout.close()
            except Exception: pass
            pr.wait()
    return n


def run_replay(engine, dirs, day, fill_mode="grid", hours=None):
    bin_s = stream_files_local(dirs, "binance", day)
    clob_s = stream_files_local(dirs, "clob", day, hours)
    return run_replay_srcs(engine, bin_s, clob_s, fill_mode)


# ---------------- live ----------------
async def run_live(engine, assets, cfg):
    import asyncio
    import urllib.request
    import urllib.parse
    try:
        import websockets
    except ImportError:
        print("websockets нет — использовать venv логгера", file=sys.stderr)
        return
    GAMMA = "https://gamma-api.polymarket.com"
    WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    # markets: asset -> {(code, slot_start): market}
    async def discover():
        now = time.time()
        ids = []
        for a in assets:
            for code, sec in (("5m", 300), ("15m", 900)):
                for slot in range(int(now // sec), int((now + 900) // sec) + 1):
                    start = slot * sec
                    if any(k[1] == start and k[0].startswith(a) for k in
                           [(m.asset, m.start) for m in engine.markets.values()]):
                        continue
                    slug = f"{a}-updown-{code}-{start}"
                    try:
                        req = urllib.request.Request(
                            f"{GAMMA}/events?" + urllib.parse.urlencode({"slug": slug}),
                            headers={"User-Agent": "kronotrade/0.1"})
                        evs = json.loads(await asyncio.get_running_loop().run_in_executor(
                            None, lambda: urllib.request.urlopen(req, timeout=8).read()))
                    except Exception:
                        continue
                    for ev in evs:
                        for mk in ev.get("markets", []):
                            toks = mk.get("clobTokenIds") or "[]"
                            if isinstance(toks, str):
                                toks = json.loads(toks)
                            if len(toks) >= 2:
                                m = Market(a, code, start, start + sec,
                                           str(toks[0]), str(toks[1]))
                                engine.register(m)
                                ids.extend(toks[:2])
                    if ids:
                        pass
        return ids

    BUS = ("wss://data-stream.binance.vision/stream?streams="
           + "/".join(f"{a}usdt@aggTrade" for a in assets))

    async def binance_worker():
        while True:
            try:
                async with websockets.connect(BUS, ping_interval=20,
                                              close_timeout=5, max_size=None) as bw:
                    print("[live] binance feed ok", file=sys.stderr, flush=True)
                    async for raw in bw:
                        try:
                            dat = (json.loads(raw) or {}).get("data") or {}
                        except Exception:
                            continue
                        if dat.get("e") == "aggTrade":
                            a = str(dat.get("s", "")).lower().replace("usdt", "")
                            if a in ("btc", "eth", "sol", "xrp"):
                                engine.minute(a, int(float(dat["T"]) / 1000),
                                              float(dat["p"]))
            except Exception as e:
                print(f"[live] binance {type(e).__name__}: {e}; retry 10s",
                      file=sys.stderr, flush=True)
                await asyncio.sleep(10 + os.urandom(1)[0] / 25.0)

    asyncio.ensure_future(binance_worker())   # спот-фид: ~100 msg/s, цикл его тянет легко

    # ===== архитектура логгера: читатель сокета НИЧЕГО не парсит =====
    # recv -> backlog(deque) -> поток-парсер -> engine. При переполнении backlog —
    # реснапшот всего набора id голым INIT-кадром (full replace, безопасно).
    backlog = collections.deque()
    CAP = 40000
    # fails = номер подряд идущего разрыва (для backoff'а). Ключ обязан быть
    # ИНИЦИАЛИЗИРОВАН: он читается в except-блоке, а тот живёт до первой здоровой
    # минуты, и KeyError там = мёртвый процесс вместо reconnect (20.09, ровно так
    # и умер на первом 1013). Проверка ключей — в tests/test_trader.py.
    live = dict(lost=0, parsed=0, resnap_at=0.0, disc_err="", fails=0)

    def _parser():
        while True:
            got = []
            for _ in range(2000):
                try:
                    got.append(backlog.popleft())
                except IndexError:
                    break
            if not got:
                time.sleep(0.005)
                continue
            for raw in got:
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                for o in (obj if isinstance(obj, list) else [obj]):
                    if isinstance(o, dict) and \
                       (o.get("event_type") or "") in ("book", "price_change"):
                        engine.book_ev(str(o.get("asset_id", "")), o)
                live["parsed"] += 1
            if live["lost"]:
                live["lost"] = 0
                live["resnap_at"] = time.time() + 3.0

    threading.Thread(target=_parser, daemon=True).start()

    while True:                                   # clob reconnect-loop
        try:
            try:
                pending = await discover()
            except Exception as e:
                live["disc_err"] = repr(e)
                pending = []
            engine.prune(time.time())
            ids = set(pending) | engine.live_ids(time.time())
            sent = set(ids)
            print(f"[live] conn: markets={engine.n_live(time.time())}/"
                  f"{len(engine.markets)} ids={len(ids)}",
                  file=sys.stderr, flush=True)
            async with websockets.connect(WS, ping_interval=20, close_timeout=5,
                                          max_size=None, max_queue=8192) as ws:
                # Дробление как у логгера (INIT на малую пачку, дальше аддитивные
                # subscribe), но шаг 0.5с, а не 3с: 3с имели смысл только пока читатель
                # стоял. Живой читатель = серверный send buffer не переполняется, и
                # 0.5с достаточно, чтобы не вываливать все снапшоты одним кадром.
                async def _reader():
                    async for msg in ws:            # ноль парсинга, ноль wait_for-оверхеда
                        if len(backlog) >= CAP:
                            live["lost"] += 1
                        else:
                            backlog.append(msg)

                # Читатель поднимается ДО подписок. Это и есть суть фикса 1013:
                # «slow consumer: send buffer full» нам говорит не «ты шлёшь слишком
                # много запросов», а «ты не читаешь то, что я уже тебе лью». Стартовый
                # снапшот на 12 токенов приходит сразу, и если в этот момент цикл
                # спит в asyncio.sleep(3) между чанками — буфер переливается, коннект
                # рвётся, reconnect подписывает ВСЁ накопленное, и дальше по кругу.
                rt = asyncio.ensure_future(_reader())
                chunks = [sorted(ids)[i:i + 12] for i in range(0, len(ids), 12)]
                if chunks:
                    await asyncio.wait_for(ws.send(json.dumps(
                        {"type": "MARKET", "assets_ids": chunks[0]})), 15)
                    for ch in chunks[1:]:
                        await asyncio.sleep(0.5)
                        await ws.send(json.dumps(
                            {"type": "MARKET", "operation": "subscribe",
                             "assets_ids": ch}))
                print(f"[live] subscribed {len(ids)} ({len(chunks)} chunks), "
                      f"рынков живых {engine.n_live(time.time())} "
                      f"всего {len(engine.markets)}",
                      file=sys.stderr, flush=True)
                last_disc = time.monotonic()
                last_beat = time.monotonic()
                parsed0 = 0
                try:
                    while True:
                        await asyncio.sleep(1.0)
                        if rt.done():
                            rt.result()             # разрыв -> наружу -> reconnect-loop
                        now = time.time()
                        engine.on_second(now)       # cp/fills по wall-clock, раз в секунду
                        if cfg["fill_mode"] == "continuous":
                            for mm in engine.markets.values():
                                engine.try_fills_live(mm, now)
                        if engine.prune(now):
                            print("[live] prune: мёртвые окна сняты с подписки",
                                  file=sys.stderr, flush=True)
                        if live["resnap_at"] and now >= live["resnap_at"]:
                            live["resnap_at"] = 0.0
                            allids = engine.live_ids(now)
                            await ws.send(json.dumps(
                                {"type": "MARKET", "assets_ids": list(allids)}))
                            print(f"[live] resnapshot {len(allids)} (backlog was full)",
                                  file=sys.stderr, flush=True)
                        if time.monotonic() - last_disc > cfg["discover_every_s"]:
                            last_disc = time.monotonic()
                            try:
                                found = await discover()
                            except Exception as e:
                                live["disc_err"] = repr(e)
                                found = []
                            new = [x for x in found if x not in sent]
                            if found:
                                print(f"[live] discover: +{len(new)} (found {len(found)})",
                                      file=sys.stderr, flush=True)
                            if new:
                                sent |= set(new)
                                await asyncio.wait_for(ws.send(json.dumps(
                                    {"type": "MARKET", "operation": "subscribe",
                                     "assets_ids": new})), 15)
                        if time.monotonic() - last_beat > 60:
                            last_beat = time.monotonic()
                            print(f"[live] {int(now)}s: parsed={live['parsed'] - parsed0}/min "
                                  f"backlog={len(backlog)} lost={live['lost']} "
                                  f"books={engine.stats['book']} cp_ok={engine.stats['cp_ok']} "
                                  f"sigwait={engine.stats['cp_nosig']} "   # σ-warm-up, не «нет сигнала»
                                  f"cp(nob={engine.stats['cp_nobench']} "
                                  f"mid={engine.stats['cp_nomid']} "
                                  f"min={engine.stats['cp_nomin']} "
                                  f"stale={engine.stats['cp_stale']}) "
                                  f"markets={engine.n_live(now)}/{len(engine.markets)} "
                                  f"intents={engine.stats['intents']} fills={engine.stats['fills']} "
                                  f"mk(res/t60/drop)={engine.stats['mk_res']}/"
                                  f"{engine.stats['mk_t60']}/{engine.stats['mk_drop']} "
                                  f"pnl={round(engine.stats['pnl'], 1)}¢"
                                  # spotage = возраст последней минуты спот-бука. Это
                                  # ГЛАВНАЯ строка для «почему у нас 66 б.п. непонятно
                                  # чего»: >120 с = фид встал, и все решения/метки с
                                  # этих минут считаются по замороженной цене (не годятся
                                  # ни в один гейт). jsize = размер journal: он обязан
                                  # только расти — уменьшился = файл подменили.
                                  f" spotage={int(time.time()) - 60 * max((b.ts[-1] for b in engine.minutes.values() if b.ts), default=0)}"
                                  + (f" jsize={os.fstat(engine.jf.fileno()).st_size}"
                                     if engine.jf else "")
                                  + (f" disc_err={live['disc_err']}" if live["disc_err"] else ""),
                                  file=sys.stderr, flush=True)
                            if live["parsed"] > parsed0:
                                # Рост за минуту -> канал жив: вчерашняя 60-секундная
                                # пауза не должна наследовать сегодняшнему первому
                                # обрыву. Сброс ТОЛЬКО на росте parsed (не по таймеру),
                                # иначе reconnect на штормовом канале успокоится до 5с.
                                live["fails"] = 0
                            parsed0 = live["parsed"]
                finally:
                    rt.cancel()
        except Exception as e:
            # Рост паузы вместо «5-10 секунд вечно». Когда канал перегружен, одинаковый
            # интервал = гарантированный повтор того же шторма; экспонента с максимумом
            # в 60 с даёт буферу улечься и не превращает день в один сплошной reconnect.
            #
            # Вся арифметика обёрнута, потому что переподключение важнее статистики:
            # сломанный счётчик обязан дать 5 секунд паузы, а не убить трейдера
            # (20.09: KeyError 'fails' из этого блока = journal молчал 40 минут).
            try:
                live["fails"] = min(live.get("fails", 0) + 1, 6)
                wait = min(60.0, 5.0 * (2 ** (live["fails"] - 1))) + os.urandom(1)[0] / 25.0
                print(f"[live] {type(e).__name__}: {e}; reconnect in {wait:.0f}s "
                      f"(попытка {live['fails']})", file=sys.stderr, flush=True)
            except Exception as e2:
                print(f"[live] {type(e).__name__}: {e} (учёт сломан: {e2!r}; "
                      f"reconnect in 5s)", file=sys.stderr, flush=True)
                wait = 5.0
            await asyncio.sleep(wait)


def load_map(eng, path, codes):
    """tokens_map.json -> engine.markets. Два формата:
    clobwin: {token: {slug,asset,code,start,up}} (YES/NO собираются по slug)
    и плоский {token: {asset,code,start,end,no}}."""
    mp = json.load(open(path))
    vals = [v for v in mp.values() if isinstance(v, dict)]
    if vals and "up" in vals[0]:
        byslug = {}
        for tok, meta in mp.items():
            if not isinstance(meta, dict) or meta.get("code") not in codes:
                continue
            try:
                byslug.setdefault(meta["slug"], {})[bool(meta["up"])] = (str(tok), meta)
            except KeyError:
                continue
        for sides in byslug.values():
            if True not in sides or False not in sides:
                continue
            (yes, my_), _ = sides[True], sides[False]
            sec = CODE_S.get(my_.get("code"), 0)
            if not sec:
                continue
            eng.register(Market(my_["asset"], my_["code"], int(my_["start"]),
                                int(my_["start"]) + sec, yes, _[0]))
    else:
        for tok, meta in mp.items():
            if not isinstance(meta, dict) or meta.get("code") not in codes:
                continue
            try:
                eng.register(Market(meta["asset"], meta["code"], int(meta["start"]),
                                    int(meta["end"]), str(tok),
                                    str(meta.get("no", tok))))
            except Exception:
                continue


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", help="каталог с kronolog-данными")
    ap.add_argument("--s3", default="", help="bucket: стриминг с S3 (aws cli на машине)")
    ap.add_argument("--prefix", default="kronolog")
    ap.add_argument("--hours", default="", help="окно реплея по часам UTC, напр. 12-14")
    ap.add_argument("--day", default=time.strftime("%Y%m%d", time.gmtime()))
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--map", default="", help="tokens_map.json для replay")
    ap.add_argument("--journal", default="/tmp/kronotrade-journal.jsonl")
    ap.add_argument("--k", type=float, default=2.5)
    ap.add_argument("--f", type=float, default=0.005)
    ap.add_argument("--bet-usd", type=float, default=50.0)
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    ap.add_argument("--fill-mode", default="grid", choices=("grid", "continuous"))
    ap.add_argument("--codes", default="5m,15m")
    ap.add_argument("--anchor", type=int, default=1)
    a = ap.parse_args()
    if a.live and "--fill-mode" not in sys.argv:
        a.fill_mode = "continuous"          # live: пересечение непрерывно; grid = replay-паритет
    cfg = dict(k=a.k, f=a.f, bet_usd=a.bet_usd, anchor=bool(a.anchor),
               fill_mode=a.fill_mode, discover_every_s=180,
               stale=(3600 if (a.replay or a.s3) else 2))
    eng = Engine(cfg, a.journal)
    if a.map:
        load_map(eng, a.map, set(a.codes.split(",")))
        print(f"map: {len(eng.markets)} YES-токенов подписки", file=sys.stderr)
    if (a.replay or a.s3) and not a.map:
        print("--replay без --map: рынков ноль. Собери карту: "
              "python3 clobwin.py --bucket B --day D --outdir /tmp/win --map-only",
              file=sys.stderr); sys.exit(3)
    if a.s3:
        import datetime
        pd_ = (datetime.datetime.strptime(a.day, "%Y%m%d")
               - datetime.timedelta(days=1)).strftime("%Y%m%d")
        bin_s = (stream_files_s3(a.s3, a.prefix, "binance", pd_, "22-23")
                 + stream_files_s3(a.s3, a.prefix, "binance", a.day))
        clob_s = stream_files_s3(a.s3, a.prefix, "clob", a.day, a.hours or None)
        print(f"s3: {len(bin_s)} binance + {len(clob_s)} clob файлов "
              f"(hours={a.hours or 'все'})", file=sys.stderr)
        n = run_replay_srcs(eng, bin_s, clob_s, fill_mode=a.fill_mode)
        print(f"replay {a.day} {a.hours or ''}: {n} строк; {eng.stats}; "
              f"fill_rate={eng.stats['fills']/max(eng.stats['intents'],1):.2f}")
    elif a.replay:
        n = run_replay(eng, a.replay, a.day, fill_mode=a.fill_mode,
                       hours=a.hours or None)
        print(f"replay {a.day}: {n} строк; {eng.stats}; "
              f"fill_rate={eng.stats['fills']/max(eng.stats['intents'],1):.2f}")
    elif a.live:
        import asyncio
        asyncio.run(run_live(eng, a.assets.split(","), cfg))
    else:
        print("нужен --replay DIR или --live", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
