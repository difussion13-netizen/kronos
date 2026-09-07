#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""audit_s3.py — быстрая сверка «то, что пишет логгер, живое и полное».

Работает из CloudShell (там есть aws-клиент и права аккаунта). Делает 3 вещи:
  1. СОШИБАЕТ файлы в бакете по потокам/дням, считает байты и GB/день;
  2. ищет ПРОВАЛЫ: ротация должна быть строго каждые 15 минут — если сетка
     рвалась (сервер падал / выгрузка не доехала), это видно;
  3. РАСПАКОВЫВАЕТ по sample свежих файлу и разбирает содержимое: сколько
     строк, какого они типа (book/price_change/tick/chainlink...), есть ли
     битые строки, «тихие места» дольше 2 минут.

Запуск:
    python3 audit_s3.py --bucket ИМЯ-БАКЕТА
    python3 audit_s3.py --bucket ... --deep      # по 3 файла на поток
Никуда ничего не пишет, только читает S3.
"""
import argparse
import gzip
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

FNAME_RE = re.compile(r"^(?P<stream>[a-z0-9]+)_(?P<day>\d{8})_(?P<time>\d{6})\.jsonl\.gz$")


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def parse_size(s):
    s = s.strip().lower()
    mult = {"kb": 1e3, "mb": 1e6, "gb": 1e9, "b": 1.0}
    m = re.match(r"^([\d.]+)\s*(kb|mb|gb|b)?$", s)
    return float(m.group(1)) * mult.get(m.group(2) or "b", 1) if m else None


def s3_ls(bucket, prefix):
    """Список файлов: (key, size_bytes, modified).兼容 ls --recursive и ls (с папок)."""
    files = []
    r = sh(f"aws s3 ls --recursive s3://{bucket}/{prefix}/")
    if r.returncode == 0 and r.stdout.strip():
        for line in r.stdout.splitlines():
            m = re.match(r"(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})\s+(\d+) (\S+)$", line.strip())
            if m:
                dt = datetime.strptime(m.group(1) + " " + m.group(2), "%Y-%m-%d %H:%M:%S")
                files.append((m.group(4), int(m.group(3)), dt.replace(tzinfo=timezone.utc)))
    if not files:  # возможно, в CLI версия без --recursive — идём по «папкам»
        r = sh(f"aws s3 ls s3://{bucket}/{prefix}/")
        for line in r.stdout.splitlines():
            m = re.match(r"PRE (.+)$", line.strip())
            if m:
                files.extend(s3_ls(bucket, prefix + "/" + m.group(1).strip("/")))
    return files


def fmt_bytes(n):
    for unit, div in (("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if n >= div:
            return f"{n/div:.2f} {unit}"
    return f"{n:.0f} B"


def classify(obj):
    """По JSON-объекту строки — к какому «виду» данных он относится."""
    if isinstance(obj, dict):
        if "event_type" in obj:
            return obj["event_type"]
        if "topic" in obj:
            return f"{obj['topic']}/{obj.get('type', '?')}"
        if "stream" in obj:
            st = obj["stream"]
            return st.split("@")[0].split("/")[0] if isinstance(st, str) else "binance"
        if "ev" in obj:
            return "meta:" + obj["ev"]
        return "dict:" + "+".join(sorted(obj.keys())[:3])
    if isinstance(obj, list) and obj:
        return "list:" + classify(obj[0])
    return type(obj).__name__


def analyze_file(path):
    """Разобрать один .jsonl.gz -> сводка."""
    n = bad = tmin = tmax = None
    kinds = Counter()
    ts = []
    try:
        with gzip.open(path, "rt", errors="replace") as f:
            for line in f:
                n = 0 if n is None else n
                n = (n or 0) + 1
                try:
                    rec = json.loads(line)
                    t = int(rec.get("t", 0))
                    if t:
                        ts.append(t)
                    if "raw" in rec:
                        kinds[classify(rec["raw"])] += 1
                    elif "text" in rec:
                        kinds["text(не-json)"] += 1
                    else:
                        kinds["конверт без raw"] += 1
                except Exception:
                    bad = (bad or 0) + 1
    except Exception as e:
        return {"error": str(e)}
    if not n:
        return {"lines": 0}
    quiet = 0.0
    if len(ts) >= 2:
        ts.sort()
        gaps = [(b - a) / 1e9 for a, b in zip(ts, ts[1:])]
        quiet = max(gaps)
        tmin, tmax = ts[0], ts[-1]
    span = (tmax - tmin) / 1e9 if tmax else 0.0
    return {"lines": n, "bad": bad or 0, "kinds": kinds,
            "span_s": span, "max_quiet_s": quiet,
            "rate_eps": (n / span) if span > 1 else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True, help="имя бакета (как в aws s3 ls)")
    ap.add_argument("--prefix", default="kronolog")
    ap.add_argument("--rotate-min", type=int, default=15, help="период ротации (config.yaml)")
    ap.add_argument("--deep", action="store_true", help="по 3 файла на поток вместо 1")
    args = ap.parse_args()

    files = s3_ls(args.bucket, args.prefix)
    if not files:
        sys.exit(f"в s3://{args.bucket}/{args.prefix}/ пусто — логгер ещё ничего не выгрузил "
                 f"(или имя/префикс не те)")

    by_stream = defaultdict(list)
    for key, size, mtime in files:
        name = key.split("/")[-1]
        m = FNAME_RE.match(name)
        stream = m.group("stream") if m else (key.split("/")[-2] if "/" in key else "?")
        start = None
        if m:
            try:
                start = datetime.strptime(m.group("day") + m.group("time"), "%Y%m%d%H%M%S") \
                          .replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        by_stream[stream].append({"key": key, "size": size, "mtime": mtime,
                                  "start": start, "day": (m.group("day") if m else str(mtime.date()))})

    now = datetime.now(timezone.utc)
    days = sorted({f["day"] for fs in by_stream.values() for f in fs})
    print(f"=== аудит kronolog: бакет {args.bucket} ===")
    print(f"дней данных: {len(days)} ({days[0]}…{days[-1]})   ротация: каждые {args.rotate_min} мин\n")

    expect = args.rotate_min * 60
    total_bytes = sum(f["size"] for fs in by_stream.values() for f in fs)
    header = f"{'поток':<10}{'файлов':>7}{'пустых':>7}{'размер':>10}{'пробелов>2.5x':>14}  последний файл"
    print(header)
    print("-" * len(header))
    for stream, fs in sorted(by_stream.items()):
        empty = sum(1 for f in fs if f["size"] < 60)
        # пробелы в сетке: только по файлам со start, до "сейчас"
        starts = sorted(f["start"] for f in fs if f["start"])
        holes = 0
        for a, b in zip(starts, starts[1:]):
            if (b - a).total_seconds() > expect * 2.5:
                holes += 1
        # если сеть рвалась в начале или хвосте — не считаем: сравниваем с последним start vs now тоже мягко
        last = max(starts) if starts else max(f["mtime"] for f in fs)
        print(f"{stream:<10}{len(fs):>7}{empty:>7}{fmt_bytes(sum(f['size'] for f in fs)):>10}"
              f"{holes:>14}  {last:%m-%d %H:%M} UTC")
    print()

    per_day = total_bytes / max(len(days), 1)
    print(f"всего: {fmt_bytes(total_bytes)}  ≈ {fmt_bytes(per_day)}/день  → "
          f"за 150 дней ≈ {per_day*150/1e9:.1f} GB, хранение ~${per_day*150/1e9*0.023:.0f}/мес S3 Standard")
    print()

    # выборочная распаковка
    tmp = "/tmp/audit_sample.jsonl.gz"
    print("образцы (свежий файл каждого потока):")
    for stream, fs in sorted(by_stream.items()):
        pick = sorted(fs, key=lambda f: f["mtime"], reverse=True)
        if args.deep and len(pick) > 2:
            pick = [pick[0], pick[len(pick)//2], pick[-1]]
        else:
            pick = pick[:1]
        for f in pick:
            r = sh(f"aws s3 cp s3://{args.bucket}/{f['key']} {tmp}")
            if r.returncode:
                print(f"  [{stream}] не скачал: {r.stderr.strip()[:80]}")
                continue
            rep = analyze_file(tmp)
            tag = f['key'].split('/')[-1]
            if "error" in rep:
                print(f"  [{stream}] {tag}: ошибка чтения {rep['error']}")
                continue
            if rep.get("lines") == 0:
                print(f"  [{stream}] {tag}: ПУСТОЙ файл (0 строк)")
                continue
            kinds = ", ".join(f"{k}:{v}" for k, v in rep["kinds"].most_common(5))
            rate = f"{rep['rate_eps']:.0f} msg/s" if rep.get("rate_eps") else "—"
            print(f"  [{stream}] {tag}: строк {rep['lines']}, зазор макс {rep['max_quiet_s']:.0f}с, "
                  f"битых {rep['bad']}, темп {rate}")
            print(f"           состав: {kinds}")

    print("\nвердикт: «пробелов>2.5x»=0 и пустые файлы ≤ пары процентов на поток — запись сплошная.")
    print("empty-файлы у rtds — норма (Chainlink шлёт только при изменении цены).")


if __name__ == "__main__":
    main()
