#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""wsprobe.py — независимый зонд Polymarket live-канала (M-авария 16.09 ночи).

Подключается СВЕЖИМ клиентом к тому же wss, на что смотрит логгер: берёт 2–4
живых токена через gamma (свежие слоты btc/eth), шлёт MARKET-subscribe и
90 секунд считает сообщения. Отвечает на один вопрос: шлёт ли БИРЖА live-поток
с этого IP вообще — или поток шлёт, а наш процесс его неconsume.

Запуск:   python3 wsprobe.py [asset ...]     (по умолчанию btc eth)
Нужен    : python>=3.9 + websockets (в venv логгера уже есть).
"""
import asyncio
import json
import re
import sys
import time
import urllib.parse
import urllib.request

GAMMA = "https://gamma-api.polymarket.com"
WSURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SEC = 300  # 5m
WIN = 90


def fresh_ids(asset: str, k: int = 2) -> list[str]:
    slot = int(time.time() // SEC)
    out: list[str] = []
    for s in (slot, slot + 1):
        slug = f"{asset}-updown-5m-{s * SEC}"
        url = f"{GAMMA}/events?" + urllib.parse.urlencode({"slug": slug})
        req = urllib.request.Request(url, headers={"User-Agent": "wsprobe/0.1"})
        try:
            evs = json.loads(urllib.request.urlopen(req, timeout=8).read())
        except Exception as e:
            print(f"  gamma {slug}: {e}")
            continue
        for ev in evs:
            for m in ev.get("markets", []):
                toks = m.get("clobTokenIds") or "[]"
                if isinstance(toks, str):
                    toks = json.loads(toks)
                out.extend(str(t) for t in toks[:k])
    return out[:4]


RX_ET = re.compile(rb'"event_type"\s*:\s*"([a-z_]+)"')


async def main(assets):
    ids: list[str] = []
    for a in assets:
        ids += fresh_ids(a)
    if not ids:
        print("нет живых токенов — gamma недоступна/изменилась; это уже ответ")
        return
    print(f"проба на {len(ids)} токенах: {ids[0][:12]}…{ids[-1][:12]}")
    import websockets
    stats: dict[str, int] = {}
    n = last = 0
    burst_n = 0
    t0 = time.time()
    try:
        async with websockets.connect(WSURL, ping_interval=20,
                                      close_timeout=5, max_size=None) as ws:
            await asyncio.wait_for(ws.send(json.dumps(
                {"auth": {}, "type": "MARKET", "assets_ids": ids})), 15)
            end = time.time() + WIN
            while time.time() < end:
                try:
                    msg = await asyncio.wait_for(ws.recv(),
                                                 timeout=max(end - time.time(), 1))
                except asyncio.TimeoutError:
                    break
                b = msg if isinstance(msg, bytes) else str(msg).encode()
                n += 1
                last = time.time()
                if time.time() - t0 < 10:
                    burst_n += 1
                for et in RX_ET.findall(b):
                    stats[et.decode()] = stats.get(et.decode(), 0) + 1
    except Exception as e:
        print(f"CONNECTION ERROR: {type(e).__name__}: {e}")
        return
    idle = round(time.time() - last, 1) if last else None
    verdict = ("ПОТОК ЕСТЬ: биржа кормит с этого IP (баг в потребителе)"
               if n - burst_n > 5 else
               "ТИШИНА ПОСЛЕ СНАПШОТА: биржа НЕ кормит с этого IP")
    print(f"msg всего={n} (снапшот-окно={burst_n}, live={n - burst_n}), "
          f"idle_после_посл.msg={idle}s, типы={stats}")
    print(verdict)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:] or ["btc", "eth"]))
