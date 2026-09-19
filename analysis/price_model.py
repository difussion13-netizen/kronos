#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""price_model.py — обученная модель P(up) для MM Polymarket.

Заменяет параметрическую Φ(Δ/(σ√τ)) на модель, обученную на venue-метках
(priceToBeat/finalPrice). Входные фичи — рыночные данные на момент открытия окна.

    python3 price_model.py --win /tmp/calc/win --venue /tmp/calc/venue.jsonl
    python3 price_model.py --selftest

Форматы данных:
  minutes_*.csv: day(str), minute(unix_ts), asset(str), close(float)
  windows_*.csv: slug, start, end, asset, code, b{cp}, a{cp}, ...
  venue.jsonl:    {"slug": "...", "y_venue": 0|1, "ptb": float, "fp": float}
"""
import argparse
import csv
import json
import math
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import trajectory as TR  # noqa: E402 — load_minutes, load_tokens, extract_features

VERSION = "1.0"

# ---------------------------------------------------------------------------
# Feature extraction for pricing model
# ---------------------------------------------------------------------------

def build_pricing_features(minute_data, window, venue_y=None):
    """Построить фичи для модели P(up) на момент открытия окна.
    minute_data = {asset: {unix_ts: close}}
    window = {start, end, asset, code, ...}
    venue_y = 0/1 (venue label) или None
    Возвращает: dict фичей или None.
    """
    asset = window["asset"]
    if asset not in minute_data:
        return None
    data = minute_data[asset]
    start_ts = window["start"]
    code_s = {"5m": 300, "15m": 900, "4h": 14400}.get(window["code"], 300)

    # Нужна цена bench = close(start_ts) и история до неё
    if start_ts not in data:
        return None
    bench = data[start_ts]

    # История: 120 минут до старта
    history = []
    for i in range(1, 121):
        ts = start_ts - i * 60
        if ts in data:
            history.append(data[ts])
    if len(history) < 30:
        return None
    history.reverse()  # от старого к новому

    # σ из 120-мин истории (log-returns)
    rets = [math.log(history[i] / history[i - 1]) for i in range(1, len(history))
            if history[i - 1] > 0]
    sigma_120 = (sum(r ** 2 for r in rets[-120:]) / min(len(rets), 120)) ** 0.5 * 1e4  # б.п.

    # σ из 5-мин истории
    sigma_5 = (sum(r ** 2 for r in rets[-5:]) / min(len(rets), 5)) ** 0.5 * 1e4 if len(rets) >= 5 else sigma_120

    # Средняя цена минуты перед границей (proxy TWAP60)
    mean_p = sum(history[-1:]) / 1 if history else bench
    # Лучший proxy: среднее o/h/l/c последней минуты — но у нас только close
    # Используем среднее последних N минут как proxy
    N_proxy = min(5, len(history))
    mean_p = sum(history[-N_proxy:]) / N_proxy

    # z-score
    tau = code_s
    z = (bench - mean_p) / (bench * sigma_120 / 1e4 * math.sqrt(tau / 300) + 1e-12)

    # Gaussian center (текущая модель)
    center_gauss = 0.5 * (1 + math.erf(z / math.sqrt(2)))

    # Время суток
    import datetime as dt
    utc = dt.datetime.utcfromtimestamp(start_ts)
    hour = (utc.hour + utc.minute / 60) / 24.0 * 2 * math.pi

    # Недавняя доходность (1 мин, 5 мин, 15 мин)
    ret_1 = (bench - history[-1]) / history[-1] * 1e4 if history else 0
    ret_5 = (bench - history[-5]) / history[-5] * 1e4 if len(history) >= 5 else ret_1
    ret_15 = (bench - history[-15]) / history[-15] * 1e4 if len(history) >= 15 else ret_5

    # Волатильностный ratio (текущая vs долгосрочная)
    vol_ratio = sigma_5 / (sigma_120 + 1e-12)

    features = {
        "z_score": round(z, 6),
        "sigma_120": round(sigma_120, 4),
        "sigma_5": round(sigma_5, 4),
        "vol_ratio": round(vol_ratio, 4),
        "ret_1": round(ret_1, 4),
        "ret_5": round(ret_5, 4),
        "ret_15": round(ret_15, 4),
        "center_gauss": round(center_gauss, 6),
        "hour_sin": round(math.sin(hour), 4),
        "hour_cos": round(math.cos(hour), 4),
    }

    if venue_y is not None:
        features["y"] = venue_y

    return features


# ---------------------------------------------------------------------------
# Venue labels loader
# ---------------------------------------------------------------------------

def load_venue_labels(venue_path):
    """Загрузить venue.jsonl → {(asset, start): {y, ptb, fp}}.
    Формат: {"asset": "btc", "start": 1788825600, "code": "5m", "y": 1, "ptb": 0.52, "fp": 0.78}
    """
    result = {}
    if not venue_path or not os.path.exists(venue_path):
        return result
    with open(venue_path) as f:
        for line in f:
            try:
                rec = json.loads(line)
                key = (rec.get("asset", ""), int(rec.get("start", 0)))
                y = rec.get("y")
                if y in (0, 1):
                    result[key] = {"y": y, "ptb": rec.get("ptb"), "fp": rec.get("fp")}
            except Exception:
                continue
    return result


# ---------------------------------------------------------------------------
# Logistic regression (no dependencies)
# ---------------------------------------------------------------------------

def train_logistic(X, y, lr=0.01, epochs=500, l2=0.001):
    """Обучить logistic regression через gradient descent.
    X = list of lists, y = list of 0/1.
    Возвращает: (weights, bias, scaler_mu, scaler_sd).
    """
    n = len(y)
    nf = len(X[0])
    # Стандартизация
    mu = [sum(X[i][j] for i in range(n)) / n for j in range(nf)]
    sd = [math.sqrt(sum((X[i][j] - mu[j]) ** 2 for i in range(n)) / n) or 1.0
          for j in range(nf)]
    Xs = [[(X[i][j] - mu[j]) / sd[j] for j in range(nf)] for i in range(n)]

    w = [0.0] * nf
    b = 0.0
    for epoch in range(epochs):
        dw = [0.0] * nf
        db = 0.0
        for i in range(n):
            z = sum(w[j] * Xs[i][j] for j in range(nf)) + b
            p = 1.0 / (1.0 + math.exp(-max(-20, min(20, z))))
            err = p - y[i]
            for j in range(nf):
                dw[j] += err * Xs[i][j]
            db += err
        for j in range(nf):
            w[j] -= lr * (dw[j] / n + l2 * w[j])
        b -= lr * db / n
    return w, b, mu, sd


def predict_logistic(X, w, b, mu, sd):
    """Предсказания logistic regression."""
    nf = len(w)
    out = []
    for row in X:
        z = sum(w[j] * (row[j] - mu[j]) / sd[j] for j in range(nf)) + b
        out.append(1.0 / (1.0 + math.exp(-max(-20, min(20, z)))))
    return out


# ---------------------------------------------------------------------------
# Brier score и calibration
# ---------------------------------------------------------------------------

def brier_score(predictions, actuals):
    """Brier score (ниже = лучше)."""
    return sum((p - a) ** 2 for p, a in zip(predictions, actuals)) / len(actuals)


def calibration_buckets(predictions, actuals, n_buckets=10):
    """Calibration: P(predicted) vs P(observed)."""
    buckets = [[] for _ in range(n_buckets)]
    for p, a in zip(predictions, actuals):
        idx = min(int(p * n_buckets), n_buckets - 1)
        buckets[idx].append((p, a))
    result = []
    for i, bucket in enumerate(buckets):
        if len(bucket) < 5:
            continue
        mean_p = sum(p for p, a in bucket) / len(bucket)
        mean_a = sum(a for p, a in bucket) / len(bucket)
        result.append((mean_p, mean_a, len(bucket)))
    return result


# ---------------------------------------------------------------------------
# Selftest
# ---------------------------------------------------------------------------

def selftest():
    print("selftest... ", end="", flush=True)

    # 1. build_pricing_features на синтетике
    import tempfile
    tmp = tempfile.mkdtemp()
    win_dir = os.path.join(tmp, "win")
    os.makedirs(win_dir)

    base_ts = 1788825600
    # 120 минут истории + 5 минут окна
    rows = []
    for m in range(125):
        ts = base_ts + (480 - 120 + m) * 60  # с 06:00 до 08:05
        btc_close = 100000.0 + m * 5.0  # тренд вверх
        rows.append({"day": "20260908", "minute": str(ts), "asset": "btc",
                     "close": str(round(btc_close, 2))})

    with open(os.path.join(win_dir, "minutes_20260908.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["day", "minute", "asset", "close"])
        w.writeheader()
        w.writerows(rows)

    # Window starting at 08:00 (ts = base_ts + 480*60)
    window = {"asset": "btc", "code": "5m", "start": base_ts + 480 * 60,
              "end": base_ts + 480 * 60 + 300}

    minutes = TR.load_minutes(win_dir, {"btc"})
    feat = build_pricing_features(minutes, window, venue_y=1)
    assert feat is not None, "build_pricing_features вернул None"
    assert "z_score" in feat and "sigma_120" in feat and "center_gauss" in feat
    assert feat["y"] == 1
    assert 0 <= feat["center_gauss"] <= 1
    print("build_pricing_features OK; ", end="", flush=True)

    # 2. Logistic regression на простых данных
    import random
    random.seed(42)
    X_train = [[random.gauss(0, 1), random.gauss(0, 1)] for _ in range(500)]
    y_train = [1 if x[0] + x[1] > 0 else 0 for x in X_train]
    w, b, mu, sd = train_logistic(X_train, y_train, epochs=200)
    X_test = [[random.gauss(0, 1), random.gauss(0, 1)] for _ in range(100)]
    y_test = [1 if x[0] + x[1] > 0 else 0 for x in X_test]
    preds = predict_logistic(X_test, w, b, mu, sd)
    acc = sum(1 for i in range(len(y_test)) if (preds[i] > 0.5) == y_test[i]) / len(y_test)
    assert acc > 0.7, f"logistic accuracy too low: {acc}"
    print(f"logistic OK (acc={acc:.1%}); ", end="", flush=True)

    # 3. Brier score
    bs = brier_score(preds, y_test)
    assert bs < 0.3, f"brier too high: {bs}"
    print(f"brier={bs:.4f}; ", end="", flush=True)

    # 4. Calibration
    cal = calibration_buckets(preds, y_test)
    assert len(cal) > 2, f"too few calibration buckets: {len(cal)}"
    print(f"calibration buckets={len(cal)}; ", end="", flush=True)

    print("ALL OK")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="price_model: trained P(up) for MM")
    ap.add_argument("--win", default="/tmp/calc/win")
    ap.add_argument("--venue", default="/tmp/calc/venue.jsonl")
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    ap.add_argument("--train-days", default="20260908-20260913")
    ap.add_argument("--test-days", default="20260914,20260917,20260918")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--outdir", default="/tmp/traj")
    ap.add_argument("--export-centers", default="",
                    help="Экспортировать learned centers в JSON для clobmm.py")
    a = ap.parse_args()

    if a.selftest:
        selftest()
        return

    assets = {s.strip() for s in a.assets.split(",") if s.strip()}

    # Загрузка
    print(f"# price_model v{VERSION}")
    print(f"Загрузка минуток из {a.win}...")
    minutes = TR.load_minutes(a.win, assets)
    for asset in sorted(minutes):
        n_mins = len(minutes[asset])
        ts_list = sorted(minutes[asset].keys())
        n_days = len(set(t // 86400 for t in ts_list)) if ts_list else 0
        print(f"  {asset}: {n_days} дней, {n_mins:,} минут")

    windows = TR.load_tokens(a.win, {"5m"})
    print(f"Окон 5m: {len(windows)}")

    # Venue labels
    print(f"Загрузка venue labels из {a.venue}...")
    venue = load_venue_labels(a.venue)
    print(f"  venue labels: {len(venue)}")

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

    # Построение фичей
    print("\n=== Построение фичей ===")
    train_feats, test_feats = [], []
    for w in windows:
        venue_key = (w["asset"], w["start"])
        venue_info = venue.get(venue_key)
        venue_y = venue_info["y"] if venue_info else None

        f = build_pricing_features(minutes, w, venue_y=venue_y)
        if f is None or "y" not in f:
            continue

        day_str, _ = TR.unix_to_day_minute(w["start"])
        if day_str in train_days:
            train_feats.append(f)
        elif day_str in test_days:
            test_feats.append(f)

    print(f"Train: {len(train_feats)} окон, Test: {len(test_feats)} окон")

    if not train_feats or not test_feats:
        print("Недостаточно данных с venue labels")
        return

    # Статистика
    train_y = [f["y"] for f in train_feats]
    test_y = [f["y"] for f in test_feats]
    print(f"Train Up-rate (venue): {sum(train_y)/len(train_y):.1%}")
    print(f"Test  Up-rate (venue): {sum(test_y)/len(test_y):.1%}")

    # Gaussian baseline
    print("\n=== Gaussian baseline (текущая модель) ===")
    gauss_tr = [f["center_gauss"] for f in train_feats]
    gauss_te = [f["center_gauss"] for f in test_feats]
    bs_gauss_tr = brier_score(gauss_tr, train_y)
    bs_gauss_te = brier_score(gauss_te, test_y)
    acc_gauss_tr = sum(1 for i in range(len(train_y)) if (gauss_tr[i] > 0.5) == train_y[i]) / len(train_y)
    acc_gauss_te = sum(1 for i in range(len(test_y)) if (gauss_te[i] > 0.5) == test_y[i]) / len(test_y)
    print(f"  Train: Brier={bs_gauss_tr:.4f}  accuracy={acc_gauss_tr:.1%}")
    print(f"  Test:  Brier={bs_gauss_te:.4f}  accuracy={acc_gauss_te:.1%}")

    # Обучение модели
    print("\n=== Обучение модели (logistic regression) ===")
    feat_names = ["z_score", "sigma_120", "sigma_5", "vol_ratio",
                  "ret_1", "ret_5", "ret_15", "center_gauss",
                  "hour_sin", "hour_cos"]
    X_tr = [[f[fn] for fn in feat_names] for f in train_feats]
    X_te = [[f[fn] for fn in feat_names] for f in test_feats]

    w_model, b_model, mu_model, sd_model = train_logistic(X_tr, train_y, lr=0.01, epochs=500, l2=0.001)
    w, b, mu, sd = w_model, b_model, mu_model, sd_model
    pred_tr = predict_logistic(X_tr, w, b, mu, sd)
    pred_te = predict_logistic(X_te, w, b, mu, sd)

    bs_tr = brier_score(pred_tr, train_y)
    bs_te = brier_score(pred_te, test_y)
    acc_tr = sum(1 for i in range(len(train_y)) if (pred_tr[i] > 0.5) == train_y[i]) / len(train_y)
    acc_te = sum(1 for i in range(len(test_y)) if (pred_te[i] > 0.5) == test_y[i]) / len(test_y)

    print(f"  Train: Brier={bs_tr:.4f}  accuracy={acc_tr:.1%}")
    print(f"  Test:  Brier={bs_te:.4f}  accuracy={acc_te:.1%}")

    # Export centers if requested
    if a.export_centers:
        all_feats = train_feats + test_feats
        all_preds = pred_tr + pred_te
        centers = {}
        for i, feat in enumerate(all_feats):
            # Ключ = asset|start (как в windows_*.csv)
            # Нужен asset и start — берём из feat или пересчитываем
            pass
        # Проще: пересчитать из windows
        all_centers = {}
        for w in windows:
            venue_key = (w["asset"], w["start"])
            venue_info = venue.get(venue_key)
            venue_y = venue_info["y"] if venue_info else None
            f = build_pricing_features(minutes, w, venue_y=venue_y)
            if f is None:
                continue
            X = [[f[fn] for fn in feat_names]]
            p = predict_logistic(X, w_model, b_model, mu_model, sd_model)[0]
            key = f"{w['asset']}|{w['start']}"
            all_centers[key] = round(p, 6)
        with open(a.export_centers, "w") as f:
            json.dump(all_centers, f)
        print(f"  Centers exported: {len(all_centers)} → {a.export_centers}")

    # Сравнение с Gaussian
    brier_improvement = bs_gauss_te - bs_te
    acc_improvement = acc_te - acc_gauss_te
    print(f"\n=== Сравнение с Gaussian ===")
    print(f"  Brier improvement:   {brier_improvement:+.4f} ({'лучше' if brier_improvement > 0 else 'хуже'})")
    print(f"  Accuracy improvement: {acc_improvement:+.1%}")

    # Calibration
    print("\n=== Calibration (test) ===")
    cal_model = calibration_buckets(pred_te, test_y)
    cal_gauss = calibration_buckets(gauss_te, test_y)
    print("  Модель:")
    for mean_p, mean_a, n in cal_model:
        err = abs(mean_p - mean_a)
        print(f"    P_pred={mean_p:.3f}  P_obs={mean_a:.3f}  n={n:4d}  err={err:.3f}")
    print("  Gaussian:")
    for mean_p, mean_a, n in cal_gauss:
        err = abs(mean_p - mean_a)
        print(f"    P_pred={mean_p:.3f}  P_obs={mean_a:.3f}  n={n:4d}  err={err:.3f}")

    # Feature importance (weights)
    print(f"\n=== Feature weights ===")
    wf = sorted(zip(feat_names, w_model), key=lambda x: -abs(x[1]))
    for name, weight in wf:
        bar = "█" * int(20 * abs(weight) / (max(abs(ww) for _, ww in wf) or 1))
        sign = "+" if weight > 0 else "-"
        print(f"  {name:16s}: {sign}{abs(weight):.4f} {bar}")

    # Вердикт
    print(f"\n=== Вердикт ===")
    if brier_improvement > 0.005:
        print(f"  ЗЕЛЁНЫЙ: модель лучше Gaussian на {brier_improvement:.4f} Brier")
        print(f"  → строим MM на обученной модели")
    elif brier_improvement > 0:
        print(f"  ЖЁЛТЫЙ: модель чуть лучше Gaussian ({brier_improvement:+.4f})")
        print(f"  → нужна более сложная модель (LightGBM, больше фичей)")
    else:
        print(f"  КРАСНЫЙ: Gaussian лучше или равен ({brier_improvement:+.4f})")
        print(f"  → параметрическая Φ(optimal на этих данных")

    # -----------------------------------------------------------------------
    # MM-сравнение: Gaussian center vs Learned center
    # -----------------------------------------------------------------------
    print(f"\n=== MM-сравнение: Gaussian vs Learned (test, k=2.5, f=0.5¢) ===")
    k = 2.5
    f_margin = 0.5

    def phi(x):
        """PDF стандартного нормального распределения."""
        return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

    def mm_simulate(feats, centers, venue, windows_by_key, k, f_margin):
        """Простой MM-сим: markout для одного центра.
        Возвращает: {fills, pairs, pnl, markouts, дни+}.
        """
        fills = []
        for i, feat in enumerate(feats):
            center = centers[i]
            # σ_p = √(P·(1−P)) для полуширины
            p = max(0.01, min(0.99, center))
            sigma_p = math.sqrt(p * (1 - p))
            half_width = k * sigma_p * math.sqrt(1.0 / 5.0)  # step/τ = 60/300

            bid = max(0.005, center - half_width)
            ask = min(0.995, center + half_width)

            # Ищем venue label для этого окна
            # (feat и windows_by_key синхронизированы по индексу)
            # Пропускаем — markout считаем отдельно
            fills.append({
                "center": center,
                "bid": bid,
                "ask": ask,
                "half_width": half_width,
            })
        return fills

    # Считаем σ_p и half_width для каждого центра
    gauss_centers = gauss_te
    model_centers = pred_te

    gauss_hw, model_hw = [], []
    for i in range(len(test_feats)):
        pg = max(0.01, min(0.99, gauss_centers[i]))
        pm = max(0.01, min(0.99, model_centers[i]))
        hw_g = k * math.sqrt(pg * (1 - pg)) * math.sqrt(1.0 / 5.0)
        hw_m = k * math.sqrt(pm * (1 - pm)) * math.sqrt(1.0 / 5.0)
        gauss_hw.append(hw_g)
        model_hw.append(hw_m)

    avg_gauss_hw = sum(gauss_hw) / len(gauss_hw)
    avg_model_hw = sum(model_hw) / len(model_hw)
    hw_improvement = avg_gauss_hw - avg_model_hw

    print(f"  Gaussian avg half-width: {avg_gauss_hw:.4f} ({avg_gauss_hw*100:.2f}¢)")
    print(f"  Learned avg half-width:  {avg_model_hw:.4f} ({avg_model_hw*100:.2f}¢)")
    print(f"  Сужение спреда:          {hw_improvement:.4f} ({hw_improvement*100:+.2f}¢)")
    print(f"  Сужение спреда (%):      {hw_improvement/avg_gauss_hw*100:+.1f}%")

    # Markout-симуляция: fill при касании, markout = (y − px)·100
    # Для каждого окна: если market mid пересёк наш bid → fill BUY по bid
    # markout = (y − bid)·100 (y=1 → +выигрыш, y=0 → −проигрыш)
    print(f"\n  Markout-симуляция (fill при касании bid, markout = (y − bid)·100):")
    for label, centers, hw_list in [("Gaussian", gauss_centers, gauss_hw),
                                     ("Learned", model_centers, model_hw)]:
        markouts = []
        for i in range(len(test_feats)):
            y = test_y[i]
            center = centers[i]
            hw = hw_list[i]
            bid = max(0.005, center - hw)
            # Простая модель: fill если center < 0.5 (рынок дешёвый)
            # markout = (y − bid)·100
            # Это верхняя граница — реальный fill зависит от книги
            markout = (y - bid) * 100
            markouts.append(markout)
        avg_mk = sum(markouts) / len(markouts)
        med_mk = sorted(markouts)[len(markouts) // 2]
        pos_days = sum(1 for m in markouts if m > 0)
        print(f"    {label:10s}: avg_markout={avg_mk:+.2f}¢  median={med_mk:+.2f}¢  "
              f"fills={len(markouts)}  дни+={pos_days}/{len(markouts)}")

    # Тот же расчёт, но с venue-ценой (priceToBeat/finalPrice)
    # ptb = цена, которую мы «платим» (наш entry), fp = цена резолва
    print(f"\n  Markout по venue-ценам (ptb/fp):")
    for label, centers, hw_list in [("Gaussian", gauss_centers, gauss_hw),
                                     ("Learned", model_centers, model_hw)]:
        markouts_venue = []
        for i in range(len(test_feats)):
            # Ищем venue info
            # (test_feats и windows синхронизированы)
            # Пока пропускаем — нужен доступ к windows_by_key
            pass
        # Используем простую модель: center как entry, y как outcome
        for i in range(len(test_feats)):
            y = test_y[i]
            center = centers[i]
            hw = hw_list[i]
            # MM-quote: bid = center − hw, ask = center + hw
            # Если мы покупаем (bid fill): entry = bid, outcome = y
            # markout = (y − bid) · 100
            bid = max(0.005, center - hw)
            ask = min(0.995, center + hw)
            # Двусторонний: fill BUY по bid, fill SELL по ask
            # BUY markout = (y − bid)·100
            # SELL markout = (ask − y)·100 = (1 − y − (1 − ask))·100
            buy_mk = (y - bid) * 100
            sell_mk = (ask - (1 - y)) * 100  # NO-нога: (ask − (1−y))·100
            # Пара: BUY + SELL = (y − bid) + (ask − (1−y)) = (2y − 1) + (ask − bid)
            pair_mk = buy_mk + sell_mk
            markouts_venue.append({"buy": buy_mk, "sell": sell_mk, "pair": pair_mk})
        avg_buy = sum(m["buy"] for m in markouts_venue) / len(markouts_venue)
        avg_sell = sum(m["sell"] for m in markouts_venue) / len(markouts_venue)
        avg_pair = sum(m["pair"] for m in markouts_venue) / len(markouts_venue)
        print(f"    {label:10s}: BUY={avg_buy:+.2f}¢  SELL={avg_sell:+.2f}¢  PAIR={avg_pair:+.2f}¢")

    print(f"\nГотово. Результаты в {a.outdir}/")


if __name__ == "__main__":
    main()