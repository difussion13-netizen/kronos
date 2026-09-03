#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""latency_bench.py — «линейка» задержек до API, которые важны торговому боту.

Только стандартная библиотека Python 3.8+; ничего ставить не надо.
Данных никуда не отправляет — только меряет и печатает.

Зачем: выбрать локацию сервера (Дублин/Лондон/Вирджиния/…), прогнав ОДИН и тот же
замер с каждого кандидата и сравнив колонку `total`.

Как читать:
  tcp   — время дотянуться до сервера (рукопожатие TCP). МЕРЯЕТСЯ ДО КРАЯ CDN
          (Cloudflare), поэтому почти всегда маленькое — само по себе не говорит
          «где origin».
  total — полный запрос-ответ. Считается edge -> внутри CDN -> origin Polymarket
          -> обратно; вот РАЗНИЦА между регионами в этой колонке и есть ответ
          на «где ставить бота».
  ws    — то же, но для websocket (именно по WS бот получает книги/тики; время
          «подключился -> первое сообщение» — самый честный показатель).

Запуск:
    python3 latency_bench.py                 # таблица
    python3 latency_bench.py --ws            # + websocket-тесты
    python3 latency_bench.py --json > bench-{локация}.json   # машиночитаемо
    python3 latency_bench.py --reps 20 --only clob_time,gamma_market
"""
import argparse
import base64
import json
import os
import socket
import ssl
import statistics
import struct
import sys
import time
import urllib.parse

UA = "kronos-latency-bench/1.0"


class BenchError(Exception):
    pass


# ---------------------------------------------------------------- helpers

def now():
    return time.perf_counter()


def readn(sock, n, deadline):
    """Прочитать ровно n байт до дедлайна (или BenchError)."""
    buf = b""
    while len(buf) < n:
        left = deadline - time.monotonic()
        if left <= 0:
            raise BenchError("timeout (read)")
        sock.settimeout(left)
        try:
            chunk = sock.recv(n - len(buf))
        except socket.timeout:
            raise BenchError("timeout (read)")
        if not chunk:
            raise BenchError("connection closed by peer")
        buf += chunk
    return buf


def read_until_double_crlf(sock, deadline):
    """Читать до \\r\\n\\r\\n (HTTP-заголовки). Возвращает (headers_bytes, остаток)."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        left = deadline - time.monotonic()
        if left <= 0:
            raise BenchError("timeout (headers)")
        sock.settimeout(left)
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            raise BenchError("timeout (headers)")
        if not chunk:
            raise BenchError("connection closed by peer")
        buf += chunk
    head, rest = buf.split(b"\r\n\r\n", 1)
    return head, rest


def tls_wrap(sock, host, insecure):
    if insecure:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    else:
        ctx = ssl.create_default_context()
        try:
            ctx.load_default_certs()  # не нужно, но не мешает; исключения глотаем ниже
        except Exception:
            pass
    try:
        return ctx.wrap_socket(sock, server_hostname=host)
    except ssl.SSLError as e:
        raise BenchError(f"TLS error: {e}")


def connect(host, port, timeout, insecure):
    """DNS+TCP+TLS. Возвращает (tls_sock, peer_ip, dns_ms, tcp_ms, tls_ms)."""
    t0 = now()
    try:
        infos = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    except socket.gaierror as e:
        raise BenchError(f"DNS failed: {e}")
    dns_ms = (now() - t0) * 1000.0
    af, styp, proto, _, sa = infos[0]
    sock = socket.socket(af, styp, proto)
    sock.settimeout(timeout)
    t0 = now()
    try:
        sock.connect(sa)
    except OSError as e:
        sock.close()
        raise BenchError(f"TCP connect failed: {e}")
    tcp_ms = (now() - t0) * 1000.0
    peer = sock.getpeername()[0]
    t0 = now()
    tls = tls_wrap(sock, host, insecure)
    tls_ms = (now() - t0) * 1000.0
    return tls, peer, dns_ms, tcp_ms, tls_ms


# ---------------------------------------------------------------- REST-замер

def measure_get(host, port, path, timeout, insecure, body_limit=4000):
    """Один GET. Возвращает dict с временем по фазам и статусом.
    body_limit — сколько байт ответа сохранить в отчёте (0 = не сохранять)."""
    tls, peer, dns_ms, tcp_ms, tls_ms = connect(host, port, timeout, insecure)
    try:
        req = (
            f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: {UA}\r\n"
            f"Accept: */*\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n"
        ).encode()
        t0 = now()
        tls.sendall(req)
        deadline = time.monotonic() + timeout
        head, rest = read_until_double_crlf(tls, deadline)
        ttfb_ms = (now() - t0) * 1000.0
        total_in = len(rest)
        body = rest
        try:
            status = int(head.split(b" ", 2)[1])
        except Exception:
            status = 0
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                raise BenchError("timeout (body)")
            tls.settimeout(left)
            chunk = tls.recv(65536)
            if not chunk:
                break
            body += chunk
            total_in += len(chunk)
            if total_in > max(64_000, body_limit * 40):
                break  # защита; замеры идут на крошечных ответах
        total_ms = (now() - t0) * 1000.0
        return dict(status=status, dns_ms=dns_ms, tcp_ms=tcp_ms, tls_ms=tls_ms,
                    ttfb_ms=ttfb_ms, total_ms=total_ms, bytes=total_in,
                    peer=peer, body=body[:body_limit].decode("utf-8", "replace"))
    finally:
        try:
            tls.close()
        except Exception:
            pass


def stats(vals):
    """min / p50 / mean / p90 / stdev для списка чисел."""
    if not vals:
        return None
    vs = sorted(vals)
    n = len(vs)
    return dict(n=n, min=round(vs[0], 2), p50=round(statistics.median(vs), 2),
                mean=round(statistics.fmean(vs), 2),
                p90=round(vs[min(n - 1, int(0.9 * n))], 2),
                stdev=round(statistics.stdev(vs), 2) if n > 1 else 0.0)


# ---------------------------------------------------------------- WebSocket-часть

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def ws_open(host, port, path, timeout, insecure):
    """TCP+TLS+upgrade. Возвращает (sock, {timing}, peer)."""
    tls, peer, dns_ms, tcp_ms, tls_ms = connect(host, port, timeout, insecure)
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
        f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\nUser-Agent: {UA}\r\n\r\n"
    ).encode()
    t0 = now()
    tls.sendall(req)
    deadline = time.monotonic() + timeout
    head, _ = read_until_double_crlf(tls, deadline)
    up_ms = (now() - t0) * 1000.0
    line = head.split(b"\r\n", 1)[0].decode(errors="replace")
    if " 101" not in line:
        tls.close()
        raise BenchError(f"websocket upgrade rejected: {line}")
    return tls, dict(dns_ms=dns_ms, tcp_ms=tcp_ms, tls_ms=tls_ms, up_ms=up_ms,
                     handshake_ms=round(tcp_ms + tls_ms + up_ms, 2)), peer


def ws_send_text(sock, text):
    payload = text.encode()
    n = len(payload)
    if n < 126:
        hdr = struct.pack("!BB", 0x81, 0x80 | n)
    elif n < 0x10000:
        hdr = struct.pack("!BBH", 0x81, 0x80 | 126, n)
    else:
        hdr = struct.pack("!BBQ", 0x81, 0x80 | 127, n)
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    sock.sendall(hdr + mask + masked)


def ws_next_text(sock, deadline):
    """Следующий текстовый фрейм от сервера (ping'и гасим pong'ами)."""
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            raise BenchError("timeout waiting ws frame")
        sock.settimeout(left)
        try:
            h = readn(sock, 2, deadline)
        except (BenchError, OSError):
            raise
        op = h[0] & 0x0F
        ln = h[1] & 0x7F
        if ln == 126:
            ln = struct.unpack("!H", readn(sock, 2, deadline))[0]
        elif ln == 127:
            ln = struct.unpack("!Q", readn(sock, 8, deadline))[0]
        payload = readn(sock, ln, deadline) if ln else b""
        if op == 0x9:  # ping -> pong
            pong = struct.pack("!BB", 0x8A, 0x80 | len(payload))
            mask = os.urandom(4)
            sock.sendall(pong + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))
            continue
        if op == 0x8:
            raise BenchError("server sent close frame")
        if op in (0x1, 0x2):
            return payload


def get_clob_tokens(timeout, insecure):
    """Пару живых token_id для подписки CLOB (тот же путь, что у логгера через gamma)."""
    r = measure_get("gamma-api.polymarket.com", 443,
                     "/markets?limit=8&active=true&closed=false&order=volume24hr"
                     "&ascending=false", timeout, insecure, body_limit=400_000)
    ids = []
    try:
        markets = json.loads(r["body"])
        if isinstance(markets, dict):
            markets = markets.get("data", [])
        for m in markets:
            raw = m.get("clobTokenIds")
            try:
                toks = json.loads(raw) if isinstance(raw, str) else (raw or [])
            except Exception:
                toks = []
            ids.extend(str(t) for t in toks)
            if len(ids) >= 32:
                break
    except Exception:
        pass
    return ids[:32]


def bench_ws_clob(timeout, insecure):
    host, port, path = "ws-subscriptions-clob.polymarket.com", 443, "/ws/market"
    try:
        tokens = get_clob_tokens(timeout, insecure)
    except Exception as e:
        tokens = []
        print(f"  [warn] gamma для токенов не ответил: {e}", file=sys.stderr)
    sock, t, peer = ws_open(host, port, path, timeout, insecure)
    out = dict(t, peer=peer, tokens=len(tokens))
    try:
        if tokens:
            t0 = now()
            ws_send_text(sock, json.dumps({"assets_ids": tokens, "type": "market"}))
            try:
                msg = ws_next_text(sock, time.monotonic() + min(30, timeout * 3))
                out["first_frame_ms"] = round((now() - t0) * 1000.0, 2)
                out["first_frame_bytes"] = len(msg)
            except BenchError as e:
                out["first_frame_ms"] = None
                out["note"] = str(e)
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return out


def bench_ws_rtds(timeout, insecure):
    host, port, path = "ws-live-data.polymarket.com", 443, "/"
    sock, t, peer = ws_open(host, port, path, timeout, insecure)
    out = dict(t, peer=peer)
    try:
        t0 = now()
        ws_send_text(sock, json.dumps({"action": "subscribe", "subscriptions": [{
            "topic": "crypto_prices_chainlink", "type": "*",
            "filters": json.dumps({"symbol": "btc/usd"})}]}))
        # RTDS болтает молча до первого изменения цены; ждём до 4x timeout
        try:
            msg = ws_next_text(sock, time.monotonic() + min(40, timeout * 4))
            out["first_frame_ms"] = round((now() - t0) * 1000.0, 2)
            out["first_frame_bytes"] = len(msg)
        except BenchError as e:
            out["first_frame_ms"] = None
            out["note"] = str(e)
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return out


# ---------------------------------------------------------------- мишени

DEFAULT_TARGETS = [
    # имя, host, path — всё GET без авторизации, ответы крошечные
    ("clob_time", "clob.polymarket.com", "/time"),
    ("gamma_market", "gamma-api.polymarket.com", "/markets?limit=1"),
    ("geoblock", "polymarket.com", "/api/geoblock"),
    ("binance_time", "api.binance.com", "/api/v3/time"),
]


def parse_extra_urls(items):
    """--url name=host[:port][/path]"""
    out = []
    for it in items or []:
        name, _, spec = it.partition("=")
        host = spec or name
        path = "/"
        if "/" in host:
            host, _, p2 = host.partition("/")
            path = "/" + p2
        port = 443
        if ":" in host:
            host, _, p2 = host.partition(":")
            port = int(p2)
        out.append((name, host, path, port))
    return out


def main():
    ap = argparse.ArgumentParser(description="Polymarket/Binance latency bench (stdlib only)")
    ap.add_argument("--reps", type=int, default=10, help="замеров на мишень (default 10)")
    ap.add_argument("--warmup", type=int, default=2, help="прогреть и выбросить (default 2)")
    ap.add_argument("--timeout", type=float, default=8.0, help="сек на операцию (default 8)")
    ap.add_argument("--only", help="только эти имена, через запятую")
    ap.add_argument("--url", action="append",
                    help="добавить мишень: name=host[:port][/path] (можно несколько раз)")
    ap.add_argument("--ws", action="store_true", help="плюс websocket-тесты (для бота это главное)")
    ap.add_argument("--json", action="store_true", help="машиночитаемый вывод")
    ap.add_argument("--tag", default=socket.gethostname(), help="имя локации для отчётов")
    ap.add_argument("--insecure", action="store_true", help="не проверять TLS-сертификат (тесты)")
    args = ap.parse_args()

    targets = [(n, h, p, 443) for (n, h, p) in DEFAULT_TARGETS]
    targets += parse_extra_urls(args.url)
    if args.only:
        wanted = {s.strip() for s in args.only.split(",")}
        targets = [t for t in targets if t[0] in wanted]

    results = {}
    geo = None
    for name, host, path, port in targets:
        samples, errs = [], []
        for i in range(args.warmup + args.reps):
            try:
                r = measure_get(host, port, path, args.timeout, args.insecure)
                if i >= args.warmup:
                    samples.append(r)
                    if name == "geoblock" and geo is None and r["status"] == 200:
                        try:
                            geo = json.loads(r["body"])
                        except Exception:
                            pass
            except BenchError as e:
                if i >= args.warmup:
                    errs.append(str(e))
            time.sleep(0.15)  # не душить CDN частыми запросами
        results[name] = dict(
            host=host, path=path, port=port, samples=samples, errors=errs,
            tcp=stats([s["tcp_ms"] for s in samples]),
            tls=stats([s["tls_ms"] for s in samples]),
            ttfb=stats([s["ttfb_ms"] for s in samples]),
            total=stats([s["total_ms"] for s in samples]),
            peers=sorted({s["peer"] for s in samples}),
            status=sorted({s["status"] for s in samples}),
        )

    ws_results = {}
    if args.ws:
        for fn, label in ((bench_ws_clob, "clob_ws"), (bench_ws_rtds, "rtds_ws")):
            try:
                ws_results[label] = fn(args.timeout, args.insecure)
            except Exception as e:
                ws_results[label] = {"error": str(e)}
            time.sleep(0.3)

    payload = dict(
        tool="kronos-latency-bench/1.0", tag=args.tag,
        host=socket.gethostname(), utc=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        args=dict(reps=args.reps, warmup=args.warmup, timeout=args.timeout),
        geo=geo, targets=results, ws=ws_results,
    )

    if args.json:
        # samples нам для пересчёта нужны, но body в них не тащим
        for t in payload["targets"].values():
            for s in t["samples"]:
                s.pop("body", None)
        print(json.dumps(payload, ensure_ascii=False, indent=1))
        return

    print(f"=== kronos latency bench ===  tag={args.tag}  host={payload['host']}  {payload['utc']} UTC")
    if geo:
        print(f"что думает Polymarket об этом IP: country={geo.get('country')!r} "
              f"region={geo.get('region')!r} blocked={geo.get('blocked')}  "
              f"(blocked=true при country=IE — норма, это про веб-фронтир, API открыт)")
    else:
        print("geoblock: ответ не получен (смотри колонку errors)")
    print()
    hdr = f"{'TARGET':<14}{'n':>3}  {'tcp ms p50(min)':<16}{'tls':<8}{'ttfb ms p50':<14}{'TOTAL ms p50  mean±sd':<24}{'errs':<5}edge"
    print(hdr)
    print("-" * len(hdr))
    for name, t in results.items():
        if t["samples"]:
            tcp, tls, ttfb, tot = t["tcp"], t["tls"], t["ttfb"], t["total"]
            print(f"{name:<14}{tot['n']:>3}  {tcp['p50']:>7.1f}({tcp['min']:.1f}) "
                  f"{tls['p50']:>6.1f}  {ttfb['p50']:>9.1f}     "
                  f"{tot['p50']:>6.1f}  {tot['mean']:>6.1f}±{tot['stdev']:<5.1f}"
                  f"{len(t['errors']):<5}{','.join(t['peers'])[:18]}")
        else:
            print(f"{name:<14}  0  {'—':<16}{'—':<8}{'—':<14}{'—':<24}{len(t['errors']):<5}"
                  f"{(t['errors'][:1] or [''])[0][:40]}")
    if args.ws:
        print()
        for label, w in ws_results.items():
            if "error" in w:
                print(f"{label:<14} ERROR: {w['error']}")
                continue
            ff = w.get("first_frame_ms")
            extra = (f"first_frame {ff:.0f} ms" if ff is not None
                     else f"first_frame нет ({w.get('note','')})")
            tk = f" tokens={w['tokens']}" if "tokens" in w else ""
            print(f"{label:<14} handshake {w['handshake_ms']:.1f} ms "
                  f"(tcp {w['tcp_ms']:.1f} + tls {w['tls_ms']:.1f} + up {w['up_ms']:.1f})  {extra}{tk}")
    print()
    print("Как читать: сравнивать колонку TOTAL между серверами (она включает путь")
    print("«край CDN -> origin»). Колонка tcp — только до ближайшего края Cloudflare,")
    print("по ней регионы не различаются. WS-строки — что реальнее всего чувствует бот.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
