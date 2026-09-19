#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_throttle — оффлайн-регрессия ConnThrottle (без сети/венv-зависимостей,
стаб yaml если не установлен). Ключевые свойства (урок 16-17.09.2026):
экспоненциальный штраф, НАСЛЕДОВАНИЕ cooldown через файл (canary-рестарт не
обнуляет), погашение кулдауна данными, сериализация connect() не ближе gap."""
import asyncio
import os
import sys
import tempfile
import time
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "logger"))
try:
    import yaml  # noqa: F401
except ImportError:
    sys.modules["yaml"] = types.SimpleNamespace(safe_load=lambda *a, **k: {})
import kronolog  # noqa: E402


async def main():
    p = os.path.join(tempfile.mkdtemp(), "throttle.json")
    th = kronolog.ConnThrottle(p, gap_s=0.01, base_s=0.1, cap_s=60.0)
    stop = asyncio.Event()
    for _ in range(3):
        th.penalty("btc-fast", "boom")
    assert th.streak == 3, th.streak
    assert th._next_ok > time.time(), "cooldown должен стоять в будущем"
    # наследование новым инстансом (симуляция рестарта процесса)
    th2 = kronolog.ConnThrottle(p, gap_s=0.01, base_s=0.1, cap_s=60.0)
    assert th2.streak == 3 and th2._next_ok > time.time()
    t0 = time.monotonic()
    await th2.acquire(stop)
    waited = time.monotonic() - t0
    assert waited > 0.1, f"acquire не дождался кулдаун ({waited})"
    # живые данные гасят штраф по одному за сообщение
    for _ in range(3):
        th2.reward()
    assert th2.streak == 0 and th2._next_ok == 0.0
    t0 = time.monotonic()
    await th2.acquire(stop)
    assert time.monotonic() - t0 < 1.0, "после обнуления acquire обязан быть быстрым"
    # cap экспоненты
    th3 = kronolog.ConnThrottle(p + ".2", gap_s=0.0, base_s=10.0, cap_s=60.0)
    for _ in range(9):
        th3.penalty("x", "y")
    assert th3._next_ok - time.time() <= 60.5, "cap не работает"
    # сериализация: параллельные acquire разнесены не меньше gap
    th4 = kronolog.ConnThrottle(p + ".3", gap_s=0.05, base_s=1, cap_s=1)
    ts = []

    async def one():
        await th4.acquire(stop)
        ts.append(time.monotonic())

    await asyncio.gather(one(), one(), one())
    gaps = [b - a for a, b in zip(ts, ts[1:])]
    assert all(g >= 0.04 for g in gaps), f"connect не сериализован: {gaps}"
    print("test_throttle: OK")


if __name__ == "__main__":
    asyncio.run(main())
