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


def load(path, horizon, binary=True, drop=()):
    with open(path, newline="") as f:
        rd = csv.DictReader(f)
        cols = rd.fieldnames
        feats = [c for c in cols if not is_label(c) and c != "t0"
                 and not any(c == d or c.startswith(d + "_") for d in drop)]
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
    ap.add_argument("--drop", default="", help="исключить фичи (через запятую; префикс снимает семейство, напр. hour)")
    ap.add_argument("--boot", type=int, default=400, help="число бустрап-пересэмплов по дневным блокам (0=выкл)")
    ap.add_argument("--wfa", action="store_true", help="walk-forward по дням: для каждого суток модели учатся только на данных до них")
    ap.add_argument("--warmup-days", type=int, default=2, help="--wfa: сколько первых UTC-суток только на обучение")
    a = ap.parse_args()
    path = os.path.join(a.dir, f"{a.asset}_{a.tf}s.csv")
    if not os.path.exists(path):
        raise SystemExit(f"нет {path} — сначала dataset.py")
    drop = tuple(x.strip() for x in a.drop.split(",") if x.strip())
    cols, feats, X, y, t = load(path, a.horizon, drop=drop)
    if drop:
        print(f"# drop: {list(drop)}")
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
    # ---- bootstrap по дневным блокам: честный разброс acc с учётом перекрытых меток
    boot = None
    if a.boot:
        import random
        days = {}
        for i, ts in enumerate(tte):
            days.setdefault(int(ts // 86400), []).append(i)
        pred = [1 if p >= 0.5 else 0 for p in predict(m, Xte)]
        ok = [int(pred[i] == yte[i]) for i in range(len(yte))]
        day_rows = list(days.values())
        rng = random.Random(7)
        accs = []
        for _ in range(a.boot):
            sel = [i for _ in range(len(day_rows)) for i in rng.choice(day_rows)]
            accs.append(sum(ok[i] for i in sel) / len(sel))
        accs.sort()
        lo = accs[int(0.10 * (len(accs) - 1))]; hi = accs[int(0.90 * (len(accs) - 1))]
        p_gt = sum(1 for x in accs if x > 0.5) / len(accs)
        boot = (lo, hi, p_gt, len(day_rows))
        print(f"bootstrap по {len(day_rows)} дн. (блок = сутки UTC): acc p10 {lo:.3f}  "
              f"p50 {accs[len(accs)//2]:.3f}  p90 {hi:.3f}   P(acc>0.5) = {p_gt:.2f}")

    if a.wfa:
        import datetime as _dt
        byday = {}
        for i in range(len(X)):
            d = _dt.datetime.utcfromtimestamp(t[i] / 1000.0 if t[i] > 1e12 else t[i]).date()
            byday.setdefault(d, []).append(i)
        days = sorted(byday)
        print(f"\n#walk-forward по дням ({len(days)} суток, warmup {a.warmup_days}):")
        accs = []
        for di, d in enumerate(days):
            if di < a.warmup_days:
                print(f"  {d}: train-only (warmup) {len(byday[d])} строк")
                continue
            tr = [i for dd in days[:di] for i in byday[dd]]
            te = byday[d]
            mm, _ = train_logreg([X[i] for i in tr], [y[i] for i in tr])
            if mm is None:
                continue
            pte = predict(mm, [X[i] for i in te])
            aa = sum((1 if p >= 0.5 else 0) == y[i] for p, i in zip(pte, te)) / len(te)
            pos = sum(y[i] for i in te) / len(te)
            accs.append(aa)
            print(f"  {d}: n={len(te):4d}  acc {aa:.3f}  (доля up {pos:.2f})")
        if accs:
            import math as _m
            mean = sum(accs) / len(accs)
            sd = _m.sqrt(sum((x - mean) ** 2 for x in accs) / max(1, len(accs) - 1))
            gt = sum(1 for x in accs if x > 0.5)
            print(f"итог: acc по дням mean {mean:.3f} ± {sd:.3f}  |  дней выше 0.5: {gt}/{len(accs)}")
            if len(accs) >= 4 and mean - sd > 0.5:
                print("вердикт wfa: перевес УСТОЙЧИВ по дням (mean-sd>0.5) — расширять окно, копить, искать утечки")
            elif len(accs) >= 4 and mean <= 0.5:
                print("вердикт wfa: по дням перевеса нет — недельный acc был флюком, закрываем горизонт")
            else:
                print("вердикт wfa: дней мало — не выдумывай, копи данные")

    gain = pm["acc"] - max(pa, 0.5)
    if boot and boot[0] > 0.5:
        verdict = ("ПЕРЕВЕС ПЕРЕЖИВАЕТ bootstrap (p10>0.5) — это уже не монетка; "
                   "беги ablation (--drop) и ищи утечки до любой радости")
    elif gain < 0.015:
        verdict = "сигнала нет (в пределах шума)"
    elif gain < 0.03:
        verdict = "слабый намёк — проверь на других --horizon/--asset"
    else:
        verdict = ("есть перевес над базлайнами — но нижняя граница bootstrap не "
                   "отличима от 0.5, т.е. неделька может врать; копить данные и "
                   "перепроверять, а не торговать")
    print(f"\nвердикт: {verdict}  (прирост acc над max(базлайнами) = {gain:+.3f})")
    print("помни: метки flat отброшены; AUC 0.5=монетка; top10% acc — как ведёт себя "
          "самая уверенная декада прогнозов (именно её торговали бы).")


if __name__ == "__main__":
    main()
