#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""smoke.py — «палочка-выручалочка»: есть ли вообще сигнал в датасете.

Честная настройка: учимся на первой части времени, тестируем на последней
(walk-forward, без подглядывания в будущее). Модель — логистическая регрессия
на 1 скрытии + два наивных базлайна. Если обобщающая точность ≈ 50% — сигнала
нет; 52–55% на недельных данных = повод бежать; >56% = скорее всего утечка,
проверяй фичи.

    python3 smoke.py --dir /tmp/ds --asset btc --horizon 5
"""
import argparse
import csv
import json
import math
import os

LABEL_COLS = ("move_bps_", "dir_", "cls_")


def is_label(c):
    return any(c.startswith(p) for p in LABEL_COLS) or c == "close"


def load(path, horizon, binary=True):
    with open(path, newline="") as f:
        rd = csv.DictReader(f)
        cols = rd.fieldnames
        feats = [c for c in cols if not is_label(c) and c != "t0"]
        X, y, t = [], [], []
        for row in rd:
            try:
                vals = [float(row[c]) for c in feats]
                mv = float(row[f"move_bps_{horizon}m"])
                cls = int(float(row[f"cls_{horizon}m"]))
            except (ValueError, KeyError):
                continue
            if binary:
                if cls == 0:          # «flat» отбрасываем: на нём учиться нечему
                    continue
                yy = 1 if cls > 0 else 0
            else:
                yy = cls
            X.append(vals); y.append(yy); t.append(float(row["t0"]))
    return cols, feats, X, y, t


def train_logreg(Xtr, ytr, lr=0.15, epochs=400, l2=1e-3):
    """Чистопython-логистическая регрессия (GD). Медленно, но надёжно."""
    n = len(Xtr)
    if n == 0:
        return None, None
    d = len(Xtr[0])
    mu = [0.0] * d; sd = [1.0] * d
    for j in range(d):
        s = sum(row[j] for row in Xtr)
        mu[j] = s / n
        v = sum((row[j] - mu[j]) ** 2 for row in Xtr) / max(1, n - 1)
        sd[j] = math.sqrt(v) or 1.0
    Z = [[(row[j] - mu[j]) / sd[j] for j in range(d)] for row in Xtr]
    w = [0.0] * d; b = 0.0
    for _ in range(epochs):
        gw = [0.0] * d; gb = 0.0
        for xi, yi in zip(Z, ytr):
            z = b + sum(wj * xj for wj, xj in zip(w, xi))
            p = 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))
            e = p - yi
            gb += e
            for j in range(d):
                gw[j] += e * xi[j]
        step = lr / n
        for j in range(d):
            w[j] -= step * gw[j] + lr * l2 * w[j]
        b -= step * gb
    return {"mu": mu, "sd": sd, "w": w, "b": b}, (Z, ytr)


def predict(m, rows):
    mu, sd, w, b = m["mu"], m["sd"], m["w"], m["b"]
    out = []
    for xi in rows:
        z = b + sum(w[j] * (xi[j] - mu[j]) / sd[j] for j in range(len(w)))
        out.append(1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z)))))
    return out


def metrics(probs, y):
    n = len(y) or 1
    acc = sum((1 if p >= 0.5 else 0) == yy for p, yy in zip(probs, y)) / n
    pos = sum(y)
    rec = sum((1 if p >= 0.5 else 0) == 1 for p, yy in zip(probs, y) if yy == 1) / max(1, pos)
    neg = sum((1 if p >= 0.5 else 0) == 0 for p, yy in zip(probs, y) if yy == 0) / max(1, n - pos)
    brier = sum((p - yy) ** 2 for p, yy in zip(probs, y)) / n
    # AUC через ранги
    pairs = sorted(zip(probs, range(n)))
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and pairs[j + 1][0] == pairs[i][0]:
            j += 1
        for k in range(i, j + 1):
            ranks[pairs[k][1]] = (i + j) / 2.0 + 1
        i = j + 1
    s1 = sum(ranks[i] for i in range(n) if y[i] == 1)
    auc = (s1 - pos * (pos + 1) / 2) / (pos * (n - pos)) if 0 < pos < n else 0.5
    # «уверенный декайль»: точность на 10% самых уверенных прогнозов
    conf = sorted(((max(p, 1 - p), (1 if p >= 0.5 else 0) == y[i], abs(p - 0.5)) for i, p in enumerate(probs)),
                  reverse=True)
    top = conf[: max(1, n // 10)]
    acc_top = sum(1 for _, ok, _ in top if ok) / len(top)
    return dict(acc=acc, bal=(rec + neg) / 2, brier=brier, auc=auc, acc_top10=acc_top)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="/tmp/ds")
    ap.add_argument("--asset", default="btc")
    ap.add_argument("--tf", type=int, default=300)
    ap.add_argument("--horizon", type=int, default=5, help="минуты")
    ap.add_argument("--holdout", type=float, default=0.3)
    ap.add_argument("--min-rows", type=int, default=200)
    a = ap.parse_args()
    path = os.path.join(a.dir, f"{a.asset}_{a.tf}s.csv")
    if not os.path.exists(path):
        raise SystemExit(f"нет {path} — сначала dataset.py")
    cols, feats, X, y, t = load(path, a.horizon)
    if len(X) < a.min_rows:
        raise SystemExit(f"строк с меткой маловато ({len(X)} < {a.min_rows}) — расширь --days "
                         f"или уменьши --min-rows (для проверки пайплайна)")
    k = int(len(X) * (1 - a.holdout))
    Xtr, ytr = X[:k], y[:k]
    Xte, yte, tte = X[k:], y[k:], t[k:]
    print(f"# smoke {a.asset} tf={a.tf}s цель dir_{a.horizon}m: {len(X)} строк "
          f"(train {k} / test {len(X)-k}), фичей {len(feats)}: {feats}")
    m, _ = train_logreg(Xtr, ytr)
    pm = metrics(predict(m, Xte), yte)
    print(f"логистическая:  acc {pm['acc']:.3f}  bal {pm['bal']:.3f}  AUC {pm['auc']:.3f}  "
          f"Brier {pm['brier']:.3f}  top10% acc {pm['acc_top10']:.3f}")
    # базлайн 1: знак r1 (моментум) — индекс 0 если есть в фичах, иначе пересчёт по close
    i_r1 = feats.index("r1") if "r1" in feats else None
    if i_r1 is not None:
        pb = metrics([0.6 if x[i_r1] > 0 else 0.4 for x in Xte], yte)
        print(f"базлайн моментум r1: acc {pb['acc']:.3f}  bal {pb['bal']:.3f}")
    # базлайн 2: большинство обучающей выборки
    maj = 1 if sum(ytr) / len(ytr) >= 0.5 else 0
    pa = sum(yy == maj for yy in yte) / len(yte)
    print(f"базлайн «всегда {maj}» (класс {sum(ytr)/len(ytr):.0%}): acc {pa:.3f}")
    gain = pm["acc"] - max(pa, 0.5)
    verdict = ("сигнала нет (в пределах шума)" if gain < 0.015 else
               "слабый намёк — проверь на других --horizon/--asset" if gain < 0.03 else
               "есть перевес над базлайнами — но на недельных данных это почти наверняка "
               "шум/утечка, перепроверяй на длинном периоде")
    print(f"\nвердикт: {verdict}  (прирост acc над max(базлайнами) = {gain:+.3f})")
    print("помни: метки flat отброшены; AUC 0.5=монетка; top10% acc — как ведёт себя "
          "самая уверенная декада прогнозов (именно её торговали бы).")


if __name__ == "__main__":
    main()
