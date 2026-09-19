#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sigma.py — E4: оправдан ли обученный слой оценки волатильности (прокси governor'а).

Цель: log1p(сумма range_bps следующих H баров) по 5м-датасету; базлайн — масштабированный
log1p(volat12) (EWMA-подобный); модель — линейная регрессия по настраиваемым фичам
(чистый python, нормальные уравнения). Сплит по времени 70/30 + по-дневный знак скилла.
Гейт: skill = 1 − MAE_model/MAE_base ≥ +10% И положителен ≥ 2/3 дней.

    python3 sigma.py --dir /tmp/ds --asset btc --horizon 12
"""
import argparse
import csv
import math
import os

FEATS = ["lv", "lr", "ar1", "ar3", "nt", "av", "hs", "hc"]


def build(path, H):
    X, Y, D = [], [], []
    with open(path) as f:
        rows = list(csv.DictReader(f))
    n = len(rows)
    for i in range(n - H):
        r = rows[i]
        try:
            vol = float(r["volat12"]); rng = float(r["range_bps"])
            fut = sum(float(rows[j]["range_bps"]) for j in range(i + 1, i + 1 + H))
            t0 = float(r["t0"])
        except (KeyError, ValueError):
            continue
        x = [math.log1p(vol), math.log1p(rng), abs(float(r["r1"])), abs(float(r["r3"])),
             float(r["n_trades"]), abs(float(r["vwap_dev_bps"])),
             float(r["hour_sin"]), float(r["hour_cos"])]
        X.append(x); Y.append(math.log1p(fut)); D.append(int(t0 // 86400))
    return X, Y, D


def ols(Xtr, Ytr, lam=1e-3):
    d = len(Xtr[0]) + 1
    XtX = [[0.0] * d for _ in range(d)]
    XtY = [0.0] * d
    for x, y in zip(Xtr, Ytr):
        xx = x + [1.0]
        for i in range(d):
            XtY[i] += xx[i] * y
            for j in range(d):
                XtX[i][j] += xx[i] * xx[j]
    for i in range(d):
        XtX[i][i] += lam
    # гаусс
    M = [XtX[i] + [XtY[i]] for i in range(d)]
    for c in range(d):
        piv = max(range(c, d), key=lambda r: abs(M[r][c]))
        M[c], M[piv] = M[piv], M[c]
        if abs(M[c][c]) < 1e-12:
            continue
        for r in range(d):
            if r == c:
                continue
            f = M[r][c] / M[c][c]
            for k in range(c, d + 1):
                M[r][k] -= f * M[c][k]
    return [M[i][d] / M[i][i] if abs(M[i][i]) > 1e-12 else 0.0 for i in range(d)]


def mae(pred, y):
    return sum(abs(p - t) for p, t in zip(pred, y)) / max(1, len(y))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="/tmp/ds")
    ap.add_argument("--asset", default="btc")
    ap.add_argument("--tf", type=int, default=300)
    ap.add_argument("--horizon", type=int, default=12, help="баров вперёд (12×5м = час)")
    a = ap.parse_args()
    path = os.path.join(a.dir, f"{a.asset}_{a.tf}s.csv")
    if not os.path.exists(path):
        raise SystemExit(f"нет {path} — сначала dataset.py")
    X, Y, D = build(path, a.horizon)
    if len(X) < 100:
        raise SystemExit(f"строк мало ({len(X)})")
    k = int(0.7 * len(X))
    w = sum(y for y in Y[:k]) / k                        # опт. сдвиг для базлайна
    base_tr = [math.log1p(12 * 1.0)] * 0                 # (не нужен)
    def base_pred(x):
        return x[0] + w
    ytr = Y[:k]; yte = Y[k:]
    pm_b = [base_pred(x) for x in X[k:]]
    beta = ols(X[:k], [y - base_pred(x) for x, y in zip(X[:k], Y[:k])])
    pm_m = [base_pred(x) + sum(b * f for b, f in zip(beta[:len(x)], x)) + beta[-1] for x in X[k:]]
    mb, mm = mae(pm_b, yte), mae(pm_m, yte)
    skill = 1 - mm / mb if mb else 0.0
    print(f"# sigma E4: {a.asset} цель=sum range след. {a.horizon} баров; тест {len(yte)} строк")
    print(f"MAE базлайн(EWMA-масштаб) {mb:.4f} | модель {mm:.4f} | skill {skill:+.1%}")
    days = {}
    for i in range(k, len(X)):
        d = D[i]
        days.setdefault(d, [0.0, 0.0, 0])
        days[d][0] += abs(pm_b[i - k] - yte[i - k]); days[d][1] += abs(pm_m[i - k] - yte[i - k])
        days[d][2] += 1
    pos = sum(1 for v in days.values() if v[0] > v[1])
    print(f"дней тестовых: {len(days)}; где модель лучше базлайна: {pos}/{len(days)}")
    for d in sorted(days):
        e_b, e_m, n = days[d]
        print(f"  день {d}: n={n:4d}  MAE base {e_b/n:.4f} vs model {e_m/n:.4f}  ({'✓' if e_b > e_m else '✗'})")
    gate = skill >= 0.10 and pos >= max(1, (2 * len(days) + 2) // 3)
    print("вердикт E4:", ("обученный σ-слой ОПРАВДАН (>=10% и устойчив по дням) — governor в план"
                          if gate else "обученный слой НЕ бьёт EWMA — Kronos-governor вычёркиваем, "
                          "MM живёт на аналитическом якоре"))


if __name__ == "__main__":
    main()
