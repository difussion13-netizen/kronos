#!/usr/bin/env python3
"""
kronolog — append-only WebSocket-логгер для исследования Polymarket 5m/15m/4h
BTC-серий и микроструктуры Binance.

Потоки (raw-вербатим, каждая строка = JSON-конверт {"t": recv_ns, "raw": ...}):
  rtds      — Polymarket RTDS: Chainlink-цены (источник резолва) + Binance-цены
  clob      — Polymarket CLOB market channel: book / price_change / last_trade
              для всех активных up/down-окон (активы x интервалы из config)
  clob_meta — журнал открытий окон (какие токены подписаны когда)
  binance   — combined streams: @aggTrade @kline_1m @depth5@100ms

Ротация: .jsonl.gz с периодом rotate_min; опциональная выгрузка в S3 (IMDS-роль,
ключей нет); повторные попытки; статус в status.json.

Запуск:  python3 kronolog.py --config config.yaml [--once 60] [--selftest 8]
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import logging
import os
import random
import signal
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import yaml

log = logging.getLogger("kronolog")

UTC = timezone.utc


def utcnow() -> datetime:
    return datetime.now(UTC)


# ---------------------------------------------------------------- S3 uploader

class S3Uploader:
    def __init__(self, cfg: dict):
        self.bucket = (cfg.get("bucket") or "").strip() or None
        self.prefix = cfg.get("prefix", "kronolog").strip("/")
        self.region = cfg.get("region") or None
        self._client = None

    @property
    def enabled(self) -> bool:
        return bool(self.bucket)

    def client(self):
        if self._client is None:
            import boto3  # ленивый импорт — selftest работает без него

            self._client = boto3.client("s3", region_name=self.region)
        return self._client

    def key_for(self, stream: str, local: Path) -> str:
        # path layout: <stream>/<YYYYMMDD>/<name>.jsonl.gz
        name = local.name
        day = name.split("_")[1][:8] if "_" in name else utcnow().strftime("%Y%m%d")
        return f"{self.prefix}/{stream}/{day}/{name}"

    def put_sync(self, stream: str, local: Path):
        with open(local, "rb") as f:
            self.client().put_object(
                Bucket=self.bucket,
                Key=self.key_for(stream, local),
                Body=f,
                ContentType="application/gzip",
            )


# ------------------------------------------------------------------- writer

class PartWriter:
    """Один поток -> ротация .jsonl.gz -> очередь выгрузки."""

    def __init__(self, name: str, root: Path, rotate_s: int, max_mb: int,
                 uploader: S3Uploader, delete_after_upload: bool):
        self.name = name
        self.dir = root / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.rotate_s = rotate_s
        self.max_bytes = max_mb * 1_000_000
        self.uploader = uploader
        self.delete_after_upload = delete_after_upload
        self._fh = None
        self._buf: list[str] = []
        self._rot_at = time.monotonic() + rotate_s
        self._part_started = int(time.time())
        self._pending: asyncio.Queue = asyncio.Queue()
        self.n_lines = 0
        self.n_files = 0
        self.n_bytes = 0

    # -- низкоуровневая часть (синхронная, дёргается из reader-loop) --------
    def _open_part(self):
        stamp = datetime.fromtimestamp(self._part_started, UTC).strftime("%Y%m%d_%H%M%S")
        path = self.dir / f"{self.name}_{stamp}.jsonl.gz"
        self._path = path
        self._fh = gzip.open(path, "wb", compresslevel=1)
        self._plines = 0
        self._pbytes = 0
        self._rot_at = time.monotonic() + self.rotate_s

    def _close_part(self):
        if self._fh is None:
            return
        self._flush_buf()
        self._fh.close()
        self._fh = None
        if getattr(self, "_plines", 0) == 0:
            self._path.unlink(missing_ok=True)
            return
        self.n_files += 1
        self._pending.put_nowait(self._path)

    def _flush_buf(self):
        # без flush()/fsync(): gzip.GzipFile буферит сам; fsync каждые 4096 строк
        # вешал event loop на десятки мс -> WS-ридер не успевал -> биржа пинала
        # 1013 slow consumer. Надёжность даёт закрытие части + выгрузка, не fsync.
        if not self._buf or self._fh is None:
            return
        data = ("\n".join(self._buf) + "\n").encode()
        self._buf.clear()
        self._pbytes = 0
        self._fh.write(data)
        self.n_bytes += len(data)

    def write_raw(self, raw: str):
        # конверт {"t":...,"raw":<оригинал>} собирается СТРОКОЙ: WS-JSOM от биржи
        # компактный и без переводов строк — повторный json.loads/dumps на каждое сообщение
        # стоил 20% CPU и был вторым источником 1013. Если текст нестандартный —
        # старый безопасный путь.
        if raw[:1] in "{[" and "\n" not in raw:
            line = '{"t":%d,"raw":%s}' % (time.time_ns(), raw)
        else:
            line = json.dumps({"t": time.time_ns(), "text": raw[:65536]}, separators=(",", ":"))
        if self._fh is None:
            self._open_part()
        self._buf.append(line)
        self.n_lines += 1
        self._plines = getattr(self, "_plines", 0) + 1
        self._pbytes = getattr(self, "_pbytes", 0) + len(line) + 1
        if self._pbytes >= 768_000:
            # 6MB -> 768кБ (17.09): инлайн-flush с gzip держал event loop до
            # полусекунды на всплесках — наш вклад в «slow consumer» на стороне
            # сервера. Мелкий квант: блокировка <60мс, файловая система та же.
            self._flush_buf()
            if self._fh.tell() >= self.max_bytes:
                self._close_part()
                self._part_started = int(time.time())
                self._open_part()

    def tick(self):
        """вызывается из upload-task: ротация по времени + flush."""
        if self._fh is not None and (time.monotonic() >= self._rot_at):
            self._close_part()
            self._part_started = int(time.time())
            self._open_part()
        else:
            self._flush_buf()

    # -- выгрузочный цикл -----------------------------------------------------
    async def upload_loop(self, stop: asyncio.Event):
        fails: dict[Path, int] = {}
        while not stop.is_set():
            try:
                path = await asyncio.wait_for(self._pending.get(), timeout=5)
            except asyncio.TimeoutError:
                for p in list(fails):
                    await self._try_upload(p, fails)
                continue
            await self._try_upload(path, fails)

    async def _try_upload(self, path: Path, fails: dict):
        if not self.uploader.enabled:
            return
        if not path.exists():
            fails.pop(path, None)
            return
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, self.uploader.put_sync, self.name, path)
            fails.pop(path, None)
            if self.delete_after_upload:
                path.unlink(missing_ok=True)
            log.info("[%s] uploaded %s", self.name, path.name)
        except Exception as e:
            n = fails.get(path, 0) + 1
            fails[path] = n
            log.warning("[%s] upload fail #%d %s: %s", self.name, n, path.name, e)

    def scan_orphans(self):
        for p in sorted(self.dir.glob("*.jsonl.gz")):
            if time.time() - p.stat().st_mtime > self.rotate_s + 120:
                self._pending.put_nowait(p)


# ------------------------------------------------------------------- metrics

class Stats:
    def __init__(self):
        self.streams: dict[str, dict] = {}

    def touch(self, name: str, key="msgs"):
        s = self.streams.setdefault(
            name, {"msgs": 0, "reconnects": 0, "last_msg": 0.0, "errs": 0})
        s[key] += 1
        s["last_msg"] = time.time()

    def err(self, name: str):
        s = self.streams.setdefault(name, {"msgs": 0, "reconnects": 0, "last_msg": 0.0, "errs": 0})
        s["errs"] += 1

    def reconn(self, name: str):
        s = self.streams.setdefault(name, {"msgs": 0, "reconnects": 0, "last_msg": 0.0, "errs": 0})
        s["reconnects"] += 1

    def snapshot(self, writers: dict[str, PartWriter]):
        now = time.time()
        out = {"ts": int(now * 1000), "streams": {}}
        for name, m in self.streams.items():
            w = writers.get(name)
            out["streams"][name] = {
                "msgs_total": m["msgs"],
                "reconnects": m["reconnects"],
                "errors": m["errs"],
                "age_last_msg_s": round(now - m["last_msg"], 1) if m["last_msg"] else None,
                "files": w.n_files if w else None,
                "lines": w.n_lines if w else None,
            }
        return out


STATS = Stats()


class ConnThrottle:
    """Глобальный «педаль-под-ковром» для reconnect к бирже (урок 16-17.09:
    slow-consumer 1013 => watchdog+canary+systemd превращали паузу биржи в
    reconnect-шторм, который эту паузу и продлевал).

    Три свойства:
    1) сериализация: одновременно подключается ОДИН коннект стрима, с шагом
       gap_s*(1+rand) — никаких N одновременных snapshot-шквалов на IP;
    2) общий кулдаун: любая неудача (1013/ошибка connect/«подписались и нам
       ничего не прислали») двигает ЭКСПОНЕНЦИАЛЬНЫЙ таймер для всех;
    3) наследуемость: streak/until живут в файле — canary-рестарт процесса
       начинает с того же кулдауна, а не с чистого лба (это и есть защита от
       «воскресного барабана»)."""

    def __init__(self, path, gap_s: float = 3.0, base_s: float = 5.0,
                 cap_s: float = 600.0):
        self.path = Path(path)
        self.gap = float(gap_s)
        self.base = float(base_s)
        self.cap = float(cap_s)
        self.streak = 0
        self._next_ok = 0.0
        self._last_conn = 0.0
        self._lock: asyncio.Lock | None = None
        try:
            d = json.loads(self.path.read_text())
            self.streak = int(d.get("streak", 0))
            self._next_ok = max(0.0, float(d.get("until", 0)))
            if self._next_ok > time.time():
                log.warning("[throttle] наследуем кулдаун %.0fs (streak %d)",
                            self._next_ok - time.time(), self.streak)
        except Exception:
            pass

    def _save(self):
        try:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"streak": self.streak,
                                        "until": round(self._next_ok, 1)}))
            tmp.replace(self.path)
        except Exception:
            pass

    async def acquire(self, stop: asyncio.Event):
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            while not stop.is_set():
                now = time.time()
                if now < self._next_ok:
                    await asyncio.sleep(min(self._next_ok - now, 60.0))
                    continue
                wait = self.gap * (1 + random.random()) - (now - self._last_conn)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_conn = time.time()
                return

    def penalty(self, who: str, why: str):
        self.streak += 1
        back = min(self.cap, self.base * (2 ** min(self.streak - 1, 7)))
        until = time.time() + back
        if until > self._next_ok:
            self._next_ok = until
        self._save()
        log.warning("[throttle] %s: %s -> общий cooldown %.0fs (streak %d)",
                    who, why[:90], back, self.streak)

    def reward(self):
        if self.streak > 0:
            self.streak -= 1
            if self.streak == 0:
                self._next_ok = 0.0
            self._save()


# ---------------------------------------------------------------- generic loop

class Stream:
    """Обёртка соединения: connect -> subscribe -> pump; watchdog+backoff."""

    def __init__(self, name: str, cfg: dict, writer: PartWriter, url: str,
                 idle_timeout: float, ping_text_interval: float | None = None,
                 ping_interval: float | None = 20.0):
        self.name = name
        self.cfg = cfg
        self.writer = writer
        self.url = url
        self.idle_timeout = idle_timeout
        self.ping_text_interval = ping_text_interval
        self.ping_interval = ping_interval
        self._subs: list[dict] = []

    async def subscribe(self, subs: list[dict]):
        self._subs = subs

    async def on_connected(self, ws):
        for m in self._subs:
            await ws.send(json.dumps(m))

    async def dynamic(self):
        """хук пере-подписки (переопределяется в ClobStream); вернуть True если reconnect."""
        return False

    async def run(self, stop: asyncio.Event):
        import websockets

        backoff = 0.5
        while not stop.is_set():
            try:
                kwargs = dict(
                    max_size=None,
                    max_queue=8192,
                    close_timeout=5,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_interval or None,
                )
                async with websockets.connect(self.url, **kwargs) as ws:
                    backoff = 0.5
                    STATS.touch(self.name, "reconnects")
                    await self.on_connected(ws)
                    await self._pump(ws, stop)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                STATS.err(self.name)
                log.warning("[%s] %s: %s", self.name, type(e).__name__, e)
            if stop.is_set():
                break
            await asyncio.sleep(min(30.0, backoff) * (1 + random.random() * 0.4))
            backoff = min(30.0, backoff * 2)

    async def _pump(self, ws, stop: asyncio.Event):
        last = time.monotonic()

        async def watchdog():
            while not stop.is_set():
                await asyncio.sleep(10)
                if time.monotonic() - last > self.idle_timeout:
                    log.warning("[%s] idle %.0fs -> reconnect", self.name,
                                time.monotonic() - last)
                    await ws.close()
                    return

        async def pinger():
            while not stop.is_set():
                await asyncio.sleep(self.ping_text_interval)
                try:
                    await ws.send("PING")
                except Exception:
                    return

        async def refresher():
            while not stop.is_set():
                await asyncio.sleep(10)
                try:
                    if await self.dynamic():
                        await ws.close()
                        return
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("[%s] dynamic: %s", self.name, e)

        tasks = [asyncio.create_task(watchdog()), asyncio.create_task(refresher())]
        if self.ping_text_interval:
            tasks.append(asyncio.create_task(pinger()))
        try:
            async for msg in ws:
                last = time.monotonic()
                if isinstance(msg, bytes):
                    msg = msg.decode("utf-8", "replace")
                if msg == "PONG":
                    # PONG — доказательство живого сокета: обновляем last,
                    # иначе «полумертвое» соединение (PING буферизуется, ответов нет)
                    # висело бы до idle_timeout без единого симптома.
                    last = time.monotonic()
                    continue
                STATS.touch(self.name)
                self.writer.write_raw(msg)
        finally:
            for t in tasks:
                t.cancel()


# ---------------------------------------------------------------- rtds

class RtdsStream(Stream):
    def __init__(self, name, cfg, writer):
        c = cfg["streams"]["rtds"]
        subs = []
        for sym in c.get("chainlink_symbols", []):
            subs.append({"action": "subscribe", "subscriptions": [{
                "topic": "crypto_prices_chainlink", "type": "*",
                # ВАЖНО: filters — это СТРОКА с JSON (не объект); см. polymarket#136
                "filters": json.dumps({"symbol": sym}),
            }]})
        bn_syms = c.get("binance_symbols") or []
        if bn_syms:
            subs.append({"action": "subscribe", "subscriptions": [{
                "topic": "crypto_prices", "type": "update",
                "filters": ",".join(bn_syms),
            }]})
        # 600 -> 90 c: RTDS шлёт PONG каждые 5 с и данные каждые ~10-20 с;
        # тишина дольше минуты = полумертвый сокет, чиним reconnect'ом сразу.
        super().__init__(name, cfg, writer, c["url"],
                         idle_timeout=90.0, ping_text_interval=5.0, ping_interval=None)
        self._init_subs = subs

    async def on_connected(self, ws):
        for m in self._init_subs:
            await ws.send(json.dumps(m))


# ---------------------------------------------------------------- binance

class BinanceStream(Stream):
    def __init__(self, name, cfg, writer):
        c = cfg["streams"]["binance"]
        streams = []
        for sym in c.get("symbols", []):
            for kind in c.get("kinds", ["aggTrade", "kline_1m", "depth5@100ms"]):
                streams.append(f"{sym}@{kind}")
        url = c["url"] + "?streams=" + "/".join(streams)
        super().__init__(name, cfg, writer, url, idle_timeout=120.0)


# ---------------------------------------------------------------- clob

class ClobStream(Stream):
    """4 соединения — по одному на актив. Единый сокет на 56-72 токена переливает
    биржевой per-connection буфер: Polymarket пинает 1013 slow consumer каждые
    ~45 с (наблюдение 2026-09-14, даже без fsync в loop). Дробим подписку сами:
    лимит считается на соединение. Discovery общий (slot_cache на всё семейство),
    subscribe/dynamic/reconnect — per-asset."""

    def __init__(self, name, cfg, writer, meta_writer):
        c = cfg["streams"]["clob"]
        self.cfgc = c
        self.meta = meta_writer
        self._assets = list(c.get("assets", ["btc"]))
        ivs0 = list(c.get("intervals", [{"code": "5m", "seconds": 300}]))
        # 16.09: btc-соединение пинали 1013 в 60 раз чаще остальных (880/48 ч) —
        # один btc-стакан переполняет пер-коннект буфер. Режем ещё и по сериям:
        # fast = самый частотный интервал, slow = остальные.
        if c.get("per_series", True) and len(ivs0) > 1:
            conns = [(f"{a}-fast", a, ivs0[:1]) for a in self._assets] + \
                    [(f"{a}-slow", a, ivs0[1:]) for a in self._assets]
        else:
            conns = [(a, a, ivs0) for a in self._assets]
        self._conn = {n: (a, iv) for n, a, iv in conns}       # name -> (asset, intervals)
        self.ws_by: dict[str, object] = {}                     # conn -> живой ws
        self.sub_ids = {n: set() for n in self._conn}
        self._last_disc = {n: 0.0 for n in self._conn}
        self._rs = {n: 0 for n in self._conn}          # курсор реснапшота
        self.slot_cache: dict = {}       # (asset, code, sec, slot) -> ids/() ; () = повторить
        self._sub_tmpl = dict(c.get("subscribe_template",
                                    {"auth": {}, "type": "MARKET", "assets_ids": []}))
        self.first_data_s = float(c.get("first_data_s", 45.0))
        self._throttle = None          # amain вставляет ConnThrottle
        # 17.09: silent stall 3.5 ч (последняя строка ~21:14 UTC 16.09, без единой
        # ошибки): half-dead коннект — протокольный ping ходит, данных нет.
        # 1800 с до watchdog — это полчаса дыры в датасете на инцидент; для
        # 48 горячих токенов простой >300 с физически невозможен, режем жёстко.
        super().__init__(name, cfg, writer, c["url"],
                         idle_timeout=float(c.get("idle_timeout", 300.0)))

    # -- supervisor: один раннер на актив --------------------------------------
    # ВАЖНО: без TaskGroup — на venv логгера (<3.11) это AttributeError, и
    # процесс входил в краш-цикл (пустой status.json 14.09 20:23+).
    async def run(self, stop: asyncio.Event):
        tasks = [asyncio.create_task(self._run_conn(n, stop), name=f"clob-{n}")
                 for n in self._conn]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    async def _run_conn(self, conn: str, stop: asyncio.Event):
        import websockets
        name = f"{self.name}:{conn}"
        # 17.09: локальный backoff заменён на ConnThrottle — общий сериализо-
        # ванный кулдаун стрима (шторм 16-17.09: 8 независимых раннеров долбили
        # дверь, которую сервер закрывал на IP; пер-коннект backoff этого не
        # видел). Локальные sleep'ы оставлены только «между», без экспоненты.
        fails = 0
        while not stop.is_set():
            if self._throttle:
                await self._throttle.acquire(stop)
                if stop.is_set():
                    break
            elif fails:
                await asyncio.sleep(min(30.0, 0.5 * (2 ** min(fails, 6)))
                                    * (1 + random.random() * 0.4))
            try:
                kwargs = dict(max_size=None, max_queue=4096, close_timeout=5,
                              ping_interval=self.ping_interval,
                              ping_timeout=self.ping_interval or None)
                async with websockets.connect(self.url, **kwargs) as ws:
                    fails = 0
                    STATS.touch(self.name, "reconnects")
                    self.ws_by[conn] = ws
                    await self.on_connected(ws, conn)
                    await self._pump_conn(ws, stop, conn, name)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                fails += 1
                STATS.err(self.name)
                log.warning("[%s] %s: %s", name, type(e).__name__, e)
                if self._throttle:
                    self._throttle.penalty(name, f"{type(e).__name__}: {e}")
            finally:
                if self.ws_by.get(conn) is not None:
                    self.ws_by.pop(conn, None)

    async def _pump_conn(self, ws, stop: asyncio.Event, conn: str, name: str):
        last = time.monotonic()
        got = False

        async def watchdog():
            while not stop.is_set():
                await asyncio.sleep(10)
                idle = time.monotonic() - last
                if not got and idle > self.first_data_s:
                    # подписка ушла, ответа нет = сервер повесил на IP очередь /
                    # отдал коннект в медленный потребитель; ждём до idle_timeout
                    # нельзя — это и был вчерашний «залп и тишина»
                    log.warning("[%s] no data %.0fs after subscribe -> reconnect",
                                name, idle)
                    if self._throttle:
                        self._throttle.penalty(name, "no first data after subscribe")
                    await ws.close()
                    return
                if got and idle > self.idle_timeout:
                    log.warning("[%s] idle %.0fs -> reconnect", name, idle)
                    await ws.close()
                    return

        async def refresher():
            while not stop.is_set():
                await asyncio.sleep(10)
                try:
                    if await self.dynamic(conn):
                        await ws.close()
                        return
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("[%s] dynamic: %s", name, e)

        tasks = [asyncio.create_task(watchdog()), asyncio.create_task(refresher())]
        try:
            async for msg in ws:
                last = time.monotonic()
                if isinstance(msg, bytes):
                    msg = msg.decode("utf-8", "replace")
                if msg == "PONG":
                    continue
                got = True
                if self._throttle:
                    self._throttle.reward()
                STATS.touch(self.name)
                self.writer.write_raw(msg)
        finally:
            for t in tasks:
                t.cancel()

    # -- discovery (sync HTTP, из executor'а) ---------------------------------
    def _http_json(self, url: str):
        req = urllib.request.Request(url, headers={"User-Agent": "kronolog/0.1"})
        with urllib.request.urlopen(req, timeout=8) as r:
            return json.loads(r.read().decode())

    def _parse_event(self, ev: dict, want_end_from: float, want_end_to: float):
        ids = []
        for m in ev.get("markets", []):
            end = m.get("endDate") or m.get("end_date_iso") or ""
            try:
                end_ts = datetime.fromisoformat(end.replace("Z", "+00:00")).timestamp()
            except Exception:
                end_ts = None
            if end_ts is not None and not (want_end_from < end_ts <= want_end_to):
                continue
            toks = m.get("clobTokenIds") or "[]"
            if isinstance(toks, str):
                toks = json.loads(toks)
            ids.extend(str(t) for t in toks)
        return ids

    def discover(self, conn: str) -> tuple[tuple[str, ...], list[dict]]:
        """Опрос Gamma ПО СЛОТАМ кэша своего коннекта (актив × набор серий).
        Пустой/ошибшийся слот переопрашивается, пока окно не истекло
        (урок 15m-дня 2026-09-14)."""
        asset, ivs = self._conn[conn]
        codes = {iv["code"] for iv in ivs}
        now = time.time()
        look = self.cfgc.get("lookahead_s", 900)
        gamma = self.cfgc.get("gamma", "https://gamma-api.polymarket.com")
        meta: list[dict] = []
        for iv in ivs:
            sec = int(iv["seconds"])
            for slot in range(int(now // sec), int((now + look) // sec) + 1):
                key = (asset, iv["code"], sec, slot)
                if self.slot_cache.get(key):
                    continue                                   # получено
                if key in self.slot_cache and now > slot * sec + sec + 600:
                    continue                                   # поздно, окно мертво
                slug = f"{asset}-updown-{iv['code']}-{slot * sec}"
                try:
                    events = self._http_json(
                        f"{gamma}/events?" + urllib.parse.urlencode({"slug": slug}))
                except Exception as e:
                    log.warning("[clob:%s] gamma %s: %s (повтор)", conn, slug, e)
                    self.slot_cache[key] = ()
                    continue
                got: set[str] = set()
                for ev in events or []:
                    got.update(self._parse_event(ev, now - 10 ** 9, now + 10 ** 9))
                self.slot_cache[key] = tuple(sorted(got))
                if got:
                    meta.append({"slug": slug, "n_tokens": len(got)})
        ids: set[str] = set()
        for key in list(self.slot_cache):
            a, code, sec, slot = key
            if slot * sec + sec < now - 120:
                del self.slot_cache[key]                       # из оборота
                continue
            if a == asset and code in codes:
                ids.update(self.slot_cache[key])
        return tuple(sorted(ids)), meta

    async def _discover_async(self, conn: str):
        return await asyncio.get_running_loop().run_in_executor(None, self.discover, conn)

    async def on_connected(self, ws, conn: str):
        ids, meta = await self._discover_async(conn)
        # 17.09: await ws.send на полуживом сокжете вешал насовсем (watchdog pump
        # ещё не запущен — 4 ч молчания после 1013). Таймаут = штатный reconnect.
        await asyncio.wait_for(
            ws.send(json.dumps({**self._sub_tmpl, "assets_ids": list(ids)})), 15)
        self.sub_ids[conn] = set(ids)
        for ev in meta:
            self.meta.write_raw(json.dumps({"ev": "subscribe", **ev}))
        log.info("[clob:%s] subscribed %d tokens", conn, len(ids))
        self._last_disc[conn] = time.monotonic()

    async def dynamic(self, conn: str):
        """Новые id коннекта доподписываются АДДИТИВНО в его сокет; reconnect —
        только при реальной ошибке сокета (шторм 14.09 больше не нужен)."""
        refresh = self.cfgc.get("discover_every_s", 45)
        if time.monotonic() - self._last_disc[conn] < refresh:
            return False
        self._last_disc[conn] = time.monotonic()
        ws = self.ws_by.get(conn)
        ids, meta = await self._discover_async(conn)
        for ev in meta:
            self.meta.write_raw(json.dumps({"ev": "discover", **ev}))
        new_ids = [t for t in ids if t not in self.sub_ids[conn]]
        if ws is None:
            return False
        if new_ids:
            try:
                # 17.09: docs (Polymarket/agent-skills, Dynamic Subscribe): в
                # живое соединение дописка шлётся С "operation":"subscribe".
                # Голый MARKET-фрейл по уже подписанным id биржа с сентября
                # трактует как замену набора — ротация resnapshot'а (16.09)
                # тем самым СВОДИЛА подписку к 12 id и убивала live-поток
                # («снапшот есть, дельт ноль» на fresh IP — диагностика 17.09).
                await asyncio.wait_for(
                    ws.send(json.dumps({**self._sub_tmpl, "operation": "subscribe",
                                        "assets_ids": new_ids})), 15)
            except Exception:
                return True                   # сокет сдох/завис — путь reconnect
            log.info("[clob:%s] +%d ids — аддитивно", conn, len(new_ids))
        # Каппинг: храним только ЖИВОЙ набор — аддитивное накопление уводило
        # подписку к сотням мёртвых токенов (16.09: eth total 288); мёртвый id
        # в живые окна не возвращается, повторная дописка исключена.
        self.sub_ids[conn] = set(ids)
        # Реснапшот-ротация: биржа НЕ шлёт книгу рынку без событий, а старый
        # билд вынужденно переподписывался на каждом реконнекте — так «мёртвые»
        # окна попадали в лог. С штормом реконнектов (14.09) этот бонус ушёл:
        # на 15.09 доехало лишь 947 id/сутки против ~3к. Компенсируем мягко:
        # каждые refresh шлём resnapshot_ids живых id по кругу — биржа отвечает
        # снапшотом на subscribe, reconnect не нужен.
        rs = int(self.cfgc.get("resnapshot_ids", 6))
        live = sorted(ids)
        if rs > 0 and live:
            off = self._rs[conn] % len(live)
            chunk = live[off:off + rs]
            if off + rs > len(live):
                chunk += live[: off + rs - len(live)]
            self._rs[conn] = off + rs
            try:
                await asyncio.wait_for(
                    ws.send(json.dumps({**self._sub_tmpl, "operation": "subscribe",
                                        "assets_ids": chunk})), 15)
            except Exception:
                return True
        return False


# ---------------------------------------------------------------- supervisor

async def amain(cfg: dict, run_for: float | None = None):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    root = Path(cfg.get("outdir", "./kronolog-out"))
    root.mkdir(parents=True, exist_ok=True)
    rot_s = int(cfg.get("rotate_min", 15)) * 60
    uploader = S3Uploader(cfg.get("s3") or {})
    delete_after = bool((cfg.get("s3") or {}).get("delete_after_upload", True))

    streams = cfg.get("streams", {})
    writers: dict[str, PartWriter] = {}

    def mk(name):
        return PartWriter(name, root, rot_s, int(cfg.get("rotate_max_mb", 256)),
                          uploader, delete_after)

    tasks = []
    meta_writer = None
    if streams.get("clob", {}).get("enabled", True):
        meta_writer = mk("clob_meta")
        writers["clob_meta"] = meta_writer

    enabled = []
    if streams.get("rtds", {}).get("enabled", True):
        w = mk("rtds"); writers["rtds"] = w
        enabled.append(RtdsStream("rtds", cfg, w))
    if streams.get("binance", {}).get("enabled", True):
        w = mk("binance"); writers["binance"] = w
        enabled.append(BinanceStream("binance", cfg, w))
    if streams.get("clob", {}).get("enabled", True):
        w = mk("clob"); writers["clob"] = w
        cs = ClobStream("clob", cfg, w, meta_writer)
        cs._throttle = ConnThrottle(
            root / "throttle.json",
            gap_s=float((streams.get("clob") or {}).get("conn_gap_s", 3.0)))
        enabled.append(cs)

    for w in writers.values():
        w.scan_orphans()

    async def ticker():
        status_path = Path(cfg.get("status_file", root / "status.json"))
        t0 = time.monotonic()
        while not stop.is_set():
            await asyncio.sleep(15)
            for w in writers.values():
                w.tick()
            snap = STATS.snapshot(writers)
            snap["up_s"] = round(time.monotonic() - t0)
            try:
                st = os.statvfs(str(root))
                snap["disk_free_gb"] = round(st.f_bavail * st.f_frsize / 1e9, 2)
            except Exception:
                pass
            tmp = status_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(snap, indent=1))
            tmp.replace(status_path)
            # Canary (авария 16.09: после 1013 on_connected завис в await ws.send —
            # pump не стартовал, watchdog не запущен, 4 часа идеальной тишины).
            # clob молчит > kill_idle_s или за 5 мин от старта не получил ни одного
            # сообщения -> добить текущие части, выгрузить в S3, os._exit(1);
            # systemd (Restart=always) поднимет чистый процесс с переподключением.
            kill_s = float(cfg.get("kill_idle_s", 900))
            if kill_s > 0 and "clob" in writers:
                m = STATS.streams.get("clob")
                age = (time.time() - m["last_msg"]) if m and m["last_msg"] else None
                fatal = (age is not None and age > kill_s) or \
                        (age is None and snap["up_s"] > 300)
                if fatal:
                    log.critical("[canary] clob age_last_msg=%s up_s=%s -> exit",
                                 age, snap["up_s"])
                    for w in writers.values():
                        try:
                            w._flush_buf()
                            w._close_part()
                        except Exception:
                            pass
                    if uploader and uploader.enabled:
                        for w in writers.values():
                            for p in sorted(w.dir.glob("*.jsonl.gz")):
                                try:
                                    await asyncio.wait_for(
                                        loop.run_in_executor(None, uploader.put_sync,
                                                             w.name, p), 20)
                                except Exception:
                                    pass
                    os._exit(1)
            if run_for and snap["up_s"] >= run_for:
                stop.set()

    async def prune():
        days = float(cfg.get("retain_days", 3))
        while not stop.is_set():
            await asyncio.sleep(3600)
            cut = time.time() - days * 86400
            for w in writers.values():
                for p in w.dir.glob("*.jsonl.gz"):
                    if p.stat().st_mtime < cut:
                        p.unlink(missing_ok=True)

    for wname, w in writers.items():
        tasks.append(asyncio.create_task(w.upload_loop(stop), name=f"up-{wname}"))
    for s in enabled:
        tasks.append(asyncio.create_task(s.run(stop), name=f"st-{s.name}"))
    tasks.append(asyncio.create_task(ticker(), name="ticker"))
    tasks.append(asyncio.create_task(prune(), name="prune"))

    log.info("kronolog up: streams=%s outdir=%s s3=%s",
             [s.name for s in enabled], root, uploader.bucket or "off")
    await stop.wait()
    log.info("stopping…")
    for t in tasks:
        t.cancel()
    for w in writers.values():
        w._close_part()


# ---------------------------------------------------------------- selftest

async def selftest(seconds: float):
    cfg = {"outdir": "./kronolog-selftest", "rotate_min": 1}
    root = Path(cfg["outdir"])
    import shutil

    shutil.rmtree(root, ignore_errors=True)
    up = S3Uploader({})
    w = PartWriter("fast", root, 2, 256, up, False)
    produced = 0
    stop = asyncio.Event()

    async def producer():
        nonlocal produced
        while not stop.is_set():
            for _ in range(200):
                w.write_raw(json.dumps({"e": "test", "i": produced}))
                produced += 1
            await asyncio.sleep(0.05)

    async def rot():
        while not stop.is_set():
            await asyncio.sleep(0.5)
            w.tick()

    t1 = asyncio.create_task(producer()); t2 = asyncio.create_task(rot())
    await asyncio.sleep(seconds)
    stop.set(); await asyncio.gather(t1, t2)
    w._close_part()
    total = 0
    for p in sorted(root.rglob("*.jsonl.gz")):
        with gzip.open(p, "rt") as f:
            n = sum(1 for line in f if json.loads(line))
        total += n
        print(f"  {p.name}: {n} lines")
    assert total == produced, f"loss: {total} != {produced}"
    print(f"SELFTEST OK: {produced} msgs, zero loss, {w.n_files} files")


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(Path(__file__).parent / "config.yaml"))
    ap.add_argument("--once", type=float, default=None,
                    help="N секунд прогона и выход (smoke-тест)")
    ap.add_argument("--selftest", type=float, default=None)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.selftest:
        asyncio.run(selftest(args.selftest))
        return

    cfg = yaml.safe_load(Path(args.config).read_text())
    asyncio.run(amain(cfg, run_for=args.once))


if __name__ == "__main__":
    main()
