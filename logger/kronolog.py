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
        if not self._buf or self._fh is None:
            return
        data = ("\n".join(self._buf) + "\n").encode()
        self._buf.clear()
        self._fh.write(data)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self.n_bytes += len(data)

    def write_raw(self, raw: str):
        try:
            parsed = json.loads(raw)
            line = json.dumps({"t": time.time_ns(), "raw": parsed}, separators=(",", ":"))
        except Exception:
            line = json.dumps({"t": time.time_ns(), "text": raw[:65536]}, separators=(",", ":"))
        if self._fh is None:
            self._open_part()
        self._buf.append(line)
        self.n_lines += 1
        self._plines = getattr(self, "_plines", 0) + 1
        if len(self._buf) >= 4096:
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
        super().__init__(name, cfg, writer, c["url"],
                         idle_timeout=600, ping_text_interval=5.0, ping_interval=None)
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
    """Динамическая подписка: каждые discover_every_s пересчитываем активные окна
    через Gamma (детерминированные слаги btc-updown-5m-<epoch> и fallback-запрос
    по series), шлём новый subscribe; на смене часа окна форсируем reconnect."""

    def __init__(self, name, cfg, writer, meta_writer):
        c = cfg["streams"]["clob"]
        self.cfgc = c
        self.meta = meta_writer
        self.cur_ids: tuple[str, ...] = ()
        self.last_discover = 0.0
        super().__init__(name, cfg, writer, c["url"], idle_timeout=1800.0)

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

    def discover(self) -> tuple[tuple[str, ...], list[dict]]:
        now = time.time()
        look = self.cfgc.get("lookahead_s", 900)
        gamma = self.cfgc.get("gamma", "https://gamma-api.polymarket.com")
        out: set[str] = set()
        meta: list[dict] = []
        for asset in self.cfgc.get("assets", ["btc"]):
            for iv in self.cfgc.get("intervals", [{"code": "5m", "seconds": 300}]):
                sec = int(iv["seconds"])
                slots = {int(now // sec), int((now + look) // sec)}
                for slot in sorted(slots):
                    slug = f"{asset}-updown-{iv['code']}-{slot * sec}"
                    try:
                        events = self._http_json(
                            f"{gamma}/events?" + urllib.parse.urlencode({"slug": slug}))
                    except Exception as e:
                        log.debug("gamma %s: %s", slug, e)
                        continue
                    if not events:
                        continue
                    for ev in events:
                        got = self._parse_event(ev, now - sec, now + look)
                        if got:
                            out.update(got)
                            meta.append({"slug": slug, "n_tokens": len(got)})
        return tuple(sorted(out)), meta

    async def _discover_async(self):
        return await asyncio.get_running_loop().run_in_executor(None, self.discover)

    async def on_connected(self, ws):
        ids, meta = await self._discover_async()
        self.cur_ids = ids
        tmpl = dict(self.cfgc.get("subscribe_template",
                                  {"auth": {}, "type": "MARKET", "assets_ids": []}))
        await ws.send(json.dumps({**tmpl, "assets_ids": list(ids)}))
        for ev in meta:
            self.meta.write_raw(json.dumps({"ev": "subscribe", **ev}))
        log.info("[clob] subscribed %d tokens", len(ids))
        self._last_disc = time.monotonic()

    async def dynamic(self):
        """Пересчёт активных окон. Любое изменение набора id -> reconnect с чистой
        подпиской (CLOB WS не поддерживает надёжный unsubscribe)."""
        refresh = self.cfgc.get("discover_every_s", 45)
        if time.monotonic() - getattr(self, "_last_disc", 0) < refresh:
            return False
        self._last_disc = time.monotonic()
        ids, meta = await self._discover_async()
        for ev in meta:
            self.meta.write_raw(json.dumps({"ev": "discover", "n_tokens": len(ids), **ev}))
        if not ids:
            return False  # discovery лёг — не трогаем живую подписку
        if ids != self.cur_ids:
            added, removed = set(ids) - set(self.cur_ids), set(self.cur_ids) - set(ids)
            log.info("[clob] ids +%d -%d (total %d) -> reconnect",
                     len(added), len(removed), len(ids))
            self.cur_ids = ids
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
        enabled.append(ClobStream("clob", cfg, w, meta_writer))

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
