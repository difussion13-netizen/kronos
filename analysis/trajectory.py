#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""trajectory.py — исследование: форма первых N минут предсказывает исход окна.

R1: trajectory features → LightGBM классификатор (направление)
R2: cross-asset lead-lag (BTC → altcoins)
R3: path-dependent volatility prediction (размах, не направление)

Данные: минутки из clobwin.py --minutes (minutes_YYYYMMDD.csv).
Формат: day(str), minute(int 0..1439), asset(str), close(float).
Окна: tokens_map.json {token_id: {start, code, asset, ...}}.

    python3 trajectory.py --win /tmp/calc/win --assets btc,eth,sol,xrp \
        --train-days 20260908-20260913 --test-days 20260914,20260917,20260918
    python3 trajectory.py --selftest
"""
import argparse
import csv
import json
import math
import os
import sys
from collections import defaultdict

VERSION = "1.0"

# ---------------------------------------------------------------------------
# Data loading (formats EXACTLY match clobwin.py output)
# ---------------------------------------------------------------------------

def load_minutes(win_dir, assets=None):
    """Загрузить минутки из minutes_YYYYMMDD.csv.
    Поле 'minute' — это unix timestamp начала минуты (НЕ номер 0..1439).
    Возвращает: {asset: {unix_ts_int: close_float}}
    """
    result = defaultdict(dict)
    for fname in sorted(os.listdir(win_dir)):
        if not fname.startswith("minutes_") or not fname.endswith(".csv"):
            continue
        path = os.path.join(win_dir, fname)
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                asset = row["asset"]
                if assets and asset not in assets:
                    continue
                ts = int(row["minute"])    # unix timestamp, НЕ 0..1439
                close = float(row["close"])
                result[asset][ts] = close
    return result


def load_tokens(win_dir, codes=None):
    """Загрузить tokens_map.json → [{start, end, code, asset, up, slug}, ...]
    start/end — unix timestamp границ окна.
    """
    path = os.path.join(win_dir, "tokens_map.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        raw = json.load(f)
    windows = []
    seen = set()
    for tok_id, info in raw.items():
        code = info.get("code", "5m")
        if codes and code not in codes:
            continue
        start = info["start"]
        key = (info["asset"], code, start)
        if key in seen:
            continue
        seen.add(key)
        code_s = {"5m": 300, "15m": 900, "4h": 14400}.get(code, 300)
        windows.append({
            "asset": info["asset"],
            "code": code,
            "start": start,
            "end": start + code_s,
            "up": info.get("up", True),
            "slug": info.get("slug", ""),
        })
    return sorted(windows, key=lambda w: w["start"])


def unix_to_day_minute(ts):
    """Unix timestamp → (day_str, minute_of_day)."""
    import datetime as dt
    utc = dt.datetime.utcfromtimestamp(ts)
    day_str = utc.strftime("%Y%m%d")
    minute = utc.hour * 60 + utc.minute
    return day_str, minute


# ---------------------------------------------------------------------------
# R1: Trajectory features
# ---------------------------------------------------------------------------

def extract_features(minute_data, window, n_obs=3):
    """Извлечь фичи из первых n_obs минут окна.
    minute_data = {asset: {unix_ts: close}}  (ключ = unix timestamp минуты)
    Возвращает: dict фичей или None если данных нет.
    """
    asset = window["asset"]
    if asset not in minute_data:
        return None
    data = minute_data[asset]

    code_s = {"5m": 300, "15m": 900, "4h": 14400}.get(window["code"], 300)
    n_minutes = code_s // 60  # 5 для 5m, 15 для 15m
    start_ts = window["start"]  # unix timestamp начала окна

    # Собрать цены: start_ts, start_ts+60, ..., start_ts+(n_minutes-1)*60
    prices = []
    for i in range(n_minutes):
        ts = start_ts + i * 60
        if ts not in data:
            return None
        prices.append(data[ts])

    if len(prices) < n_minutes:
        return None

    # Фичи из первых n_obs минут
    c = prices
    p0 = c[0]
    if p0 <= 0:
        return None

    ret_1 = (c[1] - c[0]) / c[0] * 1e4  # б.п.
    ret_2 = (c[2] - c[1]) / c[1] * 1e4 if len(c) > 2 else 0.0
    ret_full = (c[n_obs - 1] - c[0]) / c[0] * 1e4

    c_obs = c[:n_obs]
    c_max = max(c_obs)
    c_min = min(c_obs)
    c_mean = sum(c_obs) / len(c_obs)
    range_bps = (c_max - c_min) / p0 * 1e4 if p0 else 0.0

    reversal = 1.0 if (len(c) > 2 and (c[1] - c[0]) * (c[2] - c[1]) < 0) else 0.0
    accel = ret_2 - ret_1
    skew = (c[n_obs - 1] - c_mean) / ((c_max - c_min) + 1e-12)

    features = {
        "ret_1": round(ret_1, 4),
        "ret_2": round(ret_2, 4),
        "ret_full": round(ret_full, 4),
        "range_bps": round(range_bps, 4),
        "reversal": reversal,
        "accel": round(accel, 4),
        "skew": round(skew, 6),
    }

    # Таргет: направление к концу окна
    close_price = c[-1]
    features["y_dir"] = 1 if close_price > p0 else 0
    features["y_move_bps"] = round((close_price - p0) / p0 * 1e4, 4)
    features["y_vol_bps"] = round(abs(close_price - c[n_obs - 1]) / c[n_obs - 1] * 1e4, 4)

    return features


# ---------------------------------------------------------------------------
# R2: Cross-asset lead-lag
# ---------------------------------------------------------------------------

def compute_lead_lag(minute_data, base_asset="btc", alt_assets=None,
                     max_lag=5, days=None):
    """Корреляция base_ret[t] vs alt_ret[t+k].
    minute_data = {asset: {unix_ts: close}}
    Возвращает: {alt: {lag: (corr, n, p_value_approx)}}
    """
    if alt_assets is None:
        alt_assets = ["eth", "sol", "xrp"]

    if base_asset not in minute_data:
        return {}

    base_ts = sorted(minute_data[base_asset].keys())

    # Собрать пары (base_ret, alt_ret) для каждой минуты
    pairs = {a: [] for a in alt_assets}  # [(ts, base_ret, alt_ret), ...]

    for i in range(1, len(base_ts)):
        ts_prev, ts_cur = base_ts[i - 1], base_ts[i]
        if ts_cur != ts_prev + 60:      # пропуск минуты
            continue
        bv = minute_data[base_asset]
        if bv[ts_prev] <= 0:
            continue
        br = (bv[ts_cur] - bv[ts_prev]) / bv[ts_prev]
        for alt in alt_assets:
            if alt not in minute_data:
                continue
            av = minute_data[alt]
            if ts_prev in av and ts_cur in av and av[ts_prev] > 0:
                ar = (av[ts_cur] - av[ts_prev]) / av[ts_prev]
                pairs[alt].append((ts_cur, br, ar))

    results = {}
    for alt in alt_assets:
        results[alt] = {}
        pl = pairs[alt]
        for lag in range(1, max_lag + 1):
            xs, ys = [], []
            for i in range(len(pl) - lag):
                xs.append(pl[i][1])       # base_ret[i]
                ys.append(pl[i + lag][2])  # alt_ret[i+lag]
            if len(xs) < 30:
                results[alt][lag] = (0.0, len(xs), 1.0)
                continue
            n = len(xs)
            mx = sum(xs) / n
            my = sum(ys) / n
            cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)
            sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / (n - 1))
            sy = math.sqrt(sum((y - my) ** 2 for y in ys) / (n - 1))
            r = cov / (sx * sy) if sx > 0 and sy > 0 else 0.0
            t_stat = r * math.sqrt((n - 2) / (1 - r ** 2 + 1e-12))
            p = 2.0 * (1.0 - _t_cdf(abs(t_stat), n - 2))
            results[alt][lag] = (round(r, 4), n, round(p, 6))

    return results


def _t_cdf(t, df):
    """Приближение CDF t-распределения (достаточно для p<0.01)."""
    # Используем нормальное приближение при df > 30
    if df > 30:
        return _norm_cdf(t)
    # Для малых df — приближение через beta
    x = df / (df + t * t)
    # incomplete beta approximation
    return 1.0 - 0.5 * _incomplete_beta(x, df / 2.0, 0.5)


def _norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _incomplete_beta(x, a, b):
    """Грубое приближение incomplete beta (для p-value)."""
    # Симпсонова интеграция
    n_steps = 100
    dt_val = x / n_steps
    total = 0.0
    for i in range(n_steps + 1):
        t = i * dt_val
        if t <= 0 or t >= 1:
            continue
        w = 4.0 if i % 2 == 1 else 2.0
        if i == 0 or i == n_steps:
            w = 1.0
        val = (t ** (a - 1)) * ((1 - t) ** (b - 1))
        total += w * val
    total *= dt_val / 3.0
    # Нормализация на B(a,b)
    from math import gamma
    beta_ab = gamma(a) * gamma(b) / gamma(a + b)
    return total / beta_ab if beta_ab > 0 else 0.0


# ---------------------------------------------------------------------------
# R3: Path-dependent volatility
# ---------------------------------------------------------------------------

def volatility_features(features_list):
    """R3: предсказание размаха последней минуты по фичам первых 3.
    features_list = list of dicts с ret_1, range_bps, ... и y_vol_bps.
    Возвращает: (r_squared, correlation, n).
    """
    if len(features_list) < 30:
        return 0.0, 0.0, len(features_list)

    # Ridge-регрессия на 3 фичах: range_bps, accel, skew
    X_cols = ["range_bps", "accel", "skew"]
    xs = [[f.get(c, 0.0) for c in X_cols] for f in features_list]
    ys = [f["y_vol_bps"] for f in features_list]

    n = len(ys)
    # Стандартизация
    means_x = [sum(row[j] for row in xs) / n for j in range(len(X_cols))]
    stds_x = [math.sqrt(sum((row[j] - means_x[j]) ** 2 for row in xs) / n) or 1.0
              for j in range(len(X_cols))]
    xs_std = [[(row[j] - means_x[j]) / stds_x[j] for j in range(len(X_cols))]
              for row in xs]
    mean_y = sum(ys) / n
    ys_c = [y - mean_y for y in ys]

    # X'X + λI
    lam = 1.0
    XtX = [[sum(xs_std[i][j] * xs_std[i][k] for i in range(n))
            for k in range(len(X_cols))] for j in range(len(X_cols))]
    for j in range(len(X_cols)):
        XtX[j][j] += lam
    Xty = [sum(xs_std[i][j] * ys_c[i] for i in range(n)) for j in range(len(X_cols))]

    # Решение 3x3 через Крамера (достаточно для 3 фичей)
    beta = _solve_3x3(XtX, Xty)

    # Предсказания и R²
    preds = [sum(xs_std[i][j] * beta[j] for j in range(len(X_cols))) + mean_y
             for i in range(n)]
    ss_res = sum((ys[i] - preds[i]) ** 2 for i in range(n))
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    r_sq = 1.0 - ss_res / (ss_tot + 1e-12)

    # Корреляция pred/actual
    mp = sum(preds) / n
    cov_pa = sum((preds[i] - mp) * (ys[i] - mean_y) for i in range(n)) / n
    sp = math.sqrt(sum((p - mp) ** 2 for p in preds) / n)
    sy = math.sqrt(ss_tot / n)
    corr = cov_pa / (sp * sy) if sp > 0 and sy > 0 else 0.0

    return round(r_sq, 4), round(corr, 4), n


def _solve_3x3(A, b):
    """Решить 3×3 систему методом Крамера."""
    def det3(m):
        return (m[0][0] * (m[1][1] * m[2][2] - m[1][2] * m[2][1])
                - m[0][1] * (m[1][0] * m[2][2] - m[1][2] * m[2][0])
                + m[0][2] * (m[1][0] * m[2][1] - m[1][1] * m[2][0]))

    d = det3(A)
    if abs(d) < 1e-12:
        return [0.0, 0.0, 0.0]

    result = []
    for col in range(3):
        Ac = [row[:] for row in A]
        for row in range(3):
            Ac[row][col] = b[row]
        result.append(det3(Ac) / d)
    return result


# ---------------------------------------------------------------------------
# Selftest
# ---------------------------------------------------------------------------

def selftest():
    """Проверка: синтетические данные → ожидаемые результаты."""
    print("selftest... ", end="", flush=True)

    import tempfile, csv
    tmp = tempfile.mkdtemp()
    win_dir = os.path.join(tmp, "win")
    os.makedirs(win_dir)

    # Синтетические минутки: формат {day, minute(unix_ts), asset, close}
    # Базовый timestamp: 2026-09-08 00:00 UTC = 1788825600
    base_ts = 1788825600
    rows = []
    for day_idx, day in enumerate(["20260908", "20260909"]):
        day_start = base_ts + day_idx * 86400
        base_btc = 100000.0
        base_eth = 4000.0
        for m in range(50):  # 50 минут, начиная с 08:00 UTC
            ts = day_start + 480 * 60 + m * 60  # 08:00 + m минут
            btc_close = base_btc + m * 10.0 + ((m * 7) % 5 - 2)
            rows.append({"day": day, "minute": str(ts), "asset": "btc",
                         "close": str(round(btc_close, 2))})
            if m > 0:
                eth_close = base_eth + (m - 1) * 0.5 + ((m * 3) % 3 - 1)
            else:
                eth_close = base_eth
            rows.append({"day": day, "minute": str(ts), "asset": "eth",
                         "close": str(round(eth_close, 2))})
            for asset, base in [("sol", 200.0), ("xrp", 2.85)]:
                rows.append({"day": day, "minute": str(ts), "asset": asset,
                             "close": str(round(base + ((m * 13) % 7 - 3) * 0.01, 4))})

    for day in ["20260908", "20260909"]:
        with open(os.path.join(win_dir, f"minutes_{day}.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["day", "minute", "asset", "close"])
            w.writeheader()
            w.writerows([r for r in rows if r["day"] == day])

    # Tokens map: 5m окна, старт 08:00 и 08:05
    tmap = {}
    for i, start_min in enumerate([480, 485]):
        start_ts = base_ts + start_min * 60
        tmap[f"tok_{i}_up"] = {"asset": "btc", "code": "5m", "start": start_ts,
                                "up": True, "slug": f"btc-updown-5m-{start_ts}"}
        tmap[f"tok_{i}_dn"] = {"asset": "btc", "code": "5m", "start": start_ts,
                                "up": False, "slug": f"btc-updown-5m-{start_ts}"}
    with open(os.path.join(win_dir, "tokens_map.json"), "w") as f:
        json.dump(tmap, f)

    # Тест load_minutes: {asset: {unix_ts: close}}
    minutes = load_minutes(win_dir, {"btc", "eth"})
    assert "btc" in minutes and "eth" in minutes, "load_minutes не нашёл активы"
    btc_keys = sorted(minutes["btc"].keys())
    assert len(btc_keys) == 100, f"load_minutes: ожидал 100, получил {len(btc_keys)}"
    assert btc_keys[0] == base_ts + 480 * 60, f"первый ключ: {btc_keys[0]}"
    print("load_minutes OK; ", end="", flush=True)

    # Тест load_tokens
    windows = load_tokens(win_dir, {"5m"})
    assert len(windows) == 2, f"load_tokens: ожидал 2 окна, получил {len(windows)}"
    assert windows[0]["asset"] == "btc"
    print("load_tokens OK; ", end="", flush=True)

    # Тест extract_features
    features = extract_features(minutes, windows[0], n_obs=3)
    assert features is not None, "extract_features вернул None"
    assert "ret_1" in features and "y_dir" in features, "extract_features: нет фичей"
    assert isinstance(features["y_dir"], int) and features["y_dir"] in (0, 1)
    print("extract_features OK; ", end="", flush=True)

    # Тест lead-lag (btc → eth)
    ll = compute_lead_lag(minutes, "btc", ["eth"], max_lag=3)
    assert "eth" in ll, "compute_lead_lag не нашёл eth"
    assert 1 in ll["eth"], "compute_lead_lag: нет лага 1"
    r_val, n_val, p_val = ll["eth"][1]
    assert n_val > 20, f"lead-lag: мало наблюдений ({n_val})"
    print(f"lead-lag btc→eth lag=1: r={r_val}, n={n_val}; ", end="", flush=True)

    # Тест volatility_features
    feat_list = []
    for w in windows:
        f = extract_features(minutes, w, n_obs=3)
        if f:
            feat_list.append(f)
    if len(feat_list) >= 2:
        r_sq, corr, nv = volatility_features(feat_list)
        print(f"vol_features: R²={r_sq}, corr={corr}, n={nv}; ", end="", flush=True)
    else:
        print("vol_features: skipped (n<2); ", end="", flush=True)

    # Тест _solve_3x3
    A = [[4, 1, 2], [1, 3, 0], [2, 0, 5]]
    b = [7, 3, 6]
    x = _solve_3x3(A, b)
    residual = sum(abs(sum(A[i][j] * x[j] for j in range(3)) - b[i]) for i in range(3))
    assert residual < 1e-6, f"_solve_3x3: residual={residual}"
    print("_solve_3x3 OK; ", end="", flush=True)

    print("ALL OK")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="trajectory: Polymarket edge research")
    ap.add_argument("--win", default="/tmp/calc/win", help="Папка с minutes_*.csv и tokens_map.json")
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    ap.add_argument("--train-days", default="20260908-20260913",
                    help="Дни для train (диапазон или список)")
    ap.add_argument("--test-days", default="20260914,20260917,20260918",
                    help="Дни для test (список через запятую)")
    ap.add_argument("--n-obs", type=int, default=3,
                    help="Сколько минут наблюдать перед прогнозом (default 3)")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--outdir", default="/tmp/traj")
    a = ap.parse_args()

    if a.selftest:
        selftest()
        return

    assets = {s.strip() for s in a.assets.split(",") if s.strip()}

    # Загрузка
    print(f"# trajectory v{VERSION}")
    print(f"Загрузка минуток из {a.win}...")
    minutes = load_minutes(a.win, assets)
    for asset in sorted(minutes):
        n_mins = len(minutes[asset])
        # Оценить число дней из unix timestamps
        ts_list = sorted(minutes[asset].keys())
        n_days = len(set(t // 86400 for t in ts_list)) if ts_list else 0
        print(f"  {asset}: {n_days} дней, {n_mins:,} минут")

    windows = load_tokens(a.win, {"5m"})
    print(f"Окон 5m: {len(windows)}")

    # Разбивка дней
    train_days = set()
    for part in a.train_days.split(","):
        if "-" in part:
            x, y = part.split("-", 1)
            import datetime as dt
            d0 = dt.datetime.strptime(x.strip(), "%Y%m%d")
            d1 = dt.datetime.strptime(y.strip(), "%Y%m%d")
            for i in range((d1 - d0).days + 1):
                train_days.add((d0 + dt.timedelta(i)).strftime("%Y%m%d"))
        else:
            train_days.add(part.strip())
    test_days = {s.strip() for s in a.test_days.split(",") if s.strip()}
    print(f"Train: {sorted(train_days)}")
    print(f"Test:  {sorted(test_days)}")

    # R1: Извлечение фичей
    print("\n=== R1: Trajectory Features ===")
    train_feats, test_feats = [], []
    for w in windows:
        day_str, _ = unix_to_day_minute(w["start"])
        f = extract_features(minutes, w, n_obs=a.n_obs)
        if f is None:
            continue
        if day_str in train_days:
            train_feats.append(f)
        elif day_str in test_days:
            test_feats.append(f)

    print(f"Train: {len(train_feats)} окон, Test: {len(test_feats)} окон")

    if train_feats and test_feats:
        # Baseline: частота классов
        train_y = [f["y_dir"] for f in train_feats]
        test_y = [f["y_dir"] for f in test_feats]
        train_up_rate = sum(train_y) / len(train_y) if train_y else 0
        test_up_rate = sum(test_y) / len(test_y) if test_y else 0
        print(f"Train Up-rate: {train_up_rate:.1%}")
        print(f"Test  Up-rate: {test_up_rate:.1%}")

        # Простой baseline: всегда предсказывать мажоритарный класс
        majority = 1 if train_up_rate > 0.5 else 0
        baseline_acc = sum(1 for y in test_y if y == majority) / len(test_y)
        print(f"Baseline (majority): {baseline_acc:.1%}")

        # Статистика фичей
        feat_names = ["ret_1", "ret_2", "ret_full", "range_bps", "reversal", "accel", "skew"]
        print("\nФичи (train):")
        for fn in feat_names:
            vals = [f[fn] for f in train_feats]
            mn = min(vals)
            mx = max(vals)
            mu = sum(vals) / len(vals)
            print(f"  {fn:12s}: min={mn:8.3f}  mean={mu:8.3f}  max={mx:8.3f}")

        # Корреляция фичей с таргетом
        print("\nКорреляция с y_dir (train):")
        for fn in feat_names:
            vals = [f[fn] for f in train_feats]
            n = len(vals)
            mx_v = sum(vals) / n
            my_v = sum(train_y) / n
            cov_v = sum((vals[i] - mx_v) * (train_y[i] - my_v) for i in range(n)) / n
            sx_v = math.sqrt(sum((v - mx_v) ** 2 for v in vals) / n)
            sy_v = math.sqrt(sum((y - my_v) ** 2 for y in train_y) / n)
            r_v = cov_v / (sx_v * sy_v) if sx_v > 0 and sy_v > 0 else 0.0
            marker = " ***" if abs(r_v) > 0.05 else ""
            print(f"  {fn:12s}: r={r_v:+.4f}{marker}")

        # R3: Volatility prediction
        print("\n=== R3: Volatility Prediction ===")
        r_sq, corr, nv = volatility_features(train_feats)
        print(f"Train: R²={r_sq}, corr={corr}, n={nv}")
        if test_feats:
            r_sq_t, corr_t, nv_t = volatility_features(test_feats)
            print(f"Test:  R²={r_sq_t}, corr={corr_t}, n={nv_t}")

    # R2: Lead-lag
    print("\n=== R2: Cross-Asset Lead-Lag ===")
    all_days = sorted(train_days | test_days)
    ll = compute_lead_lag(minutes, "btc", ["eth", "sol", "xrp"],
                          max_lag=5, days=all_days)
    for alt in sorted(ll):
        print(f"\nbtc → {alt}:")
        for lag in sorted(ll[alt]):
            r, n, p = ll[alt][lag]
            sig = " ***" if p < 0.01 else " *" if p < 0.05 else ""
            print(f"  lag={lag}: r={r:+.4f}  n={n}  p={p:.6f}{sig}")

    # R1.5: Классификатор (LightGBM если есть, иначе logistic regression)
    if train_feats and test_feats:
        print("\n=== R1.5: Классификатор (направление) ===")
        feat_names = ["ret_1", "ret_2", "ret_full", "range_bps", "reversal", "accel", "skew"]
        X_tr = [[f[fn] for fn in feat_names] for f in train_feats]
        y_tr = [f["y_dir"] for f in train_feats]
        X_te = [[f[fn] for fn in feat_names] for f in test_feats]
        y_te = [f["y_dir"] for f in test_feats]

        try:
            import lightgbm as lgb
            ds = lgb.Dataset(X_tr, label=y_tr, feature_name=feat_names)
            params = {"objective": "binary", "metric": "binary_logloss",
                      "num_leaves": 15, "max_depth": 5, "learning_rate": 0.1,
                      "min_data_in_leaf": 50, "verbose": -1, "seed": 42}
            model = lgb.train(params, ds, num_boost_round=200)
            proba_tr = model.predict(X_tr)
            proba_te = model.predict(X_te)
            fi = dict(zip(feat_names, model.feature_importance(importance_type="gain")))
            print("  Модель: LightGBM (200 деревьев, depth=5)")
            model_name = "LightGBM"
        except ImportError:
            print("  lightgbm не найден → logistic regression (gradient descent)")
            model_name = "LogReg"
            # Стандартизация
            n_f = len(feat_names)
            n_tr = len(X_tr)
            mu = [sum(X_tr[i][j] for i in range(n_tr)) / n_tr for j in range(n_f)]
            sd = [math.sqrt(sum((X_tr[i][j] - mu[j]) ** 2 for i in range(n_tr)) / n_tr) or 1.0
                  for j in range(n_f)]
            Xs_tr = [[(X_tr[i][j] - mu[j]) / sd[j] for j in range(n_f)] for i in range(n_tr)]
            Xs_te = [[(X_te[i][j] - mu[j]) / sd[j] for j in range(n_f)] for i in range(len(X_te))]
            # Gradient descent
            w = [0.0] * n_f
            b = 0.0
            lr = 0.01
            for epoch in range(500):
                dw = [0.0] * n_f
                db = 0.0
                for i in range(n_tr):
                    z = sum(w[j] * Xs_tr[i][j] for j in range(n_f)) + b
                    p = 1.0 / (1.0 + math.exp(-max(-20, min(20, z))))
                    err = p - y_tr[i]
                    for j in range(n_f):
                        dw[j] += err * Xs_tr[i][j]
                    db += err
                for j in range(n_f):
                    w[j] -= lr * (dw[j] / n_tr + 0.001 * w[j])
                b -= lr * db / n_tr
            def predict_logreg(Xs):
                out = []
                for row in Xs:
                    z = sum(w[j] * row[j] for j in range(n_f)) + b
                    out.append(1.0 / (1.0 + math.exp(-max(-20, min(20, z)))))
                return out
            proba_tr = predict_logreg(Xs_tr)
            proba_te = predict_logreg(Xs_te)
            fi = dict(zip(feat_names, [abs(w[j]) for j in range(n_f)]))

        # Accuracy
        def acc(proba, y, thr=0.5):
            pred = [1 if p > thr else 0 for p in proba]
            return sum(1 for i in range(len(y)) if pred[i] == y[i]) / len(y)

        train_acc = acc(proba_tr, y_tr)
        test_acc = acc(proba_te, y_te)
        print(f"\n  Train accuracy (thr=0.5): {train_acc:.1%}")
        print(f"  Test  accuracy (thr=0.5): {test_acc:.1%}")
        print(f"  Baseline (majority):      {baseline_acc:.1%}")
        edge = test_acc - baseline_acc
        verdict = "ЗЕЛЁНЫЙ (edge>5%)" if edge > 0.05 else "ЖЁЛТЫЙ (2-5%)" if edge > 0.02 else "КРАСНЫЙ (<=2%)"
        print(f"  Edge vs baseline:         {edge:+.1%} → {verdict}")

        # Precision at high confidence
        print("\n  Precision by confidence bucket (test):")
        buckets = [(0.5, 0.55), (0.55, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 1.01)]
        for lo, hi in buckets:
            idx = [i for i in range(len(proba_te)) if lo <= proba_te[i] < hi]
            if len(idx) < 5:
                continue
            correct = sum(1 for i in idx if y_te[i] == (1 if proba_te[i] > 0.5 else 0))
            n_bucket = len(idx)
            prec = correct / n_bucket
            rate = n_bucket / len(proba_te)
            print(f"    P∈[{lo:.2f},{hi:.2f}): n={n_bucket:4d} ({rate:5.1%})  precision={prec:.1%}")

        # High-confidence edge
        hi_conf = [i for i in range(len(proba_te)) if proba_te[i] > 0.65 or proba_te[i] < 0.35]
        if hi_conf:
            hc_correct = sum(1 for i in hi_conf
                            if y_te[i] == (1 if proba_te[i] > 0.5 else 0))
            hc_prec = hc_correct / len(hi_conf)
            hc_rate = len(hi_conf) / len(proba_te)
            print(f"\n  High-confidence |P-0.5|>0.15: n={len(hi_conf)} ({hc_rate:.1%})  "
                  f"precision={hc_prec:.1%}")

        # Feature importance
        fi_sorted = sorted(fi.items(), key=lambda x: -x[1])
        total_fi = sum(v for _, v in fi_sorted) or 1
        print(f"\n  Feature importance ({model_name}):")
        for name, val in fi_sorted:
            bar = "█" * int(30 * val / total_fi)
            print(f"    {name:12s}: {val/total_fi:5.1%} {bar}")

        # EV estimate
        if hi_conf and len(hi_conf) > 20:
            # Assume: enter when |P-0.5|>0.15, hold 2 min, exit on close
            # Binance fee: maker 0.02% × 2 = 0.04%
            # Expected gain per correct trade: ~range_bps/2 (average move)
            avg_range = sum(train_feats[i]["range_bps"] for i in range(len(train_feats))) / len(train_feats)
            ev_correct = avg_range / 2  # half the range on average
            ev_per_trade = (hc_prec * ev_correct - (1 - hc_prec) * ev_correct - 0.4)  # minus fees
            trades_per_day = len(hi_conf) / 3  # 3 test days
            print(f"\n  EV estimate:")
            print(f"    avg range: {avg_range:.1f} bps")
            print(f"    ev/trade:  {ev_per_trade:+.2f} bps (after 0.4 bps round-trip fee)")
            print(f"    trades/day: ~{trades_per_day:.0f}")
            if ev_per_trade > 0:
                print(f"    → POSITIVE EV: ~{ev_per_trade * trades_per_day:.0f} bps/day")
            else:
                print(f"    → NEGATIVE EV (fees > edge)")

    os.makedirs(a.outdir, exist_ok=True)
    print(f"\nГотово. Результаты в {a.outdir}/")


if __name__ == "__main__":
    main()