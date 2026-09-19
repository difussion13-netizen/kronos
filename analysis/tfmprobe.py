#!/usr/bin/env python3
"""tfmprobe.py — офлайн-проверка: даёт ли TimesFM (zero-shot) навык над
нашими базовыми моделями на том горизонте, который реально нужен updown-маркет-мейкеру.

Метрики на скользящих окнах 1m-клайнов Binance (log-close, σ в тех же шагах):

  P(up): Brier / лог-лосс для
           zero  = 0.5                       — наш «нет дрейфа» режим,
           raw   = Phi(z) с z = mom*h/(σ√h)   — наивная экстраполяция импульса,
           cal   = Phi(κ·z), κ подогнан на ПЕРВОЙ половине, меряется на ВТОРОЙ
                   (out-of-sample Platt-шкала: честный потолок «линейного» сигнала),
           tfm   = Phi((med-last)/σ_q) из квантиль-хэда TimesFM;
         + hit — доля окон, где знак 5-мин импульса совпал со знаком следующего r_h.
  vol:   σMAE = mean|σ_ewma·√h − |r_h|| в б.п. (именно √h: h-шаговый σ масштабится
         как √h; линейное h завышало метрику на больших h), mean|r_h| для контекста.

Калибровка харнеса (проверено 18.09, 200k окон):
  * чистое случайное блуждание (навыка нет): Brier zero 0.2500, Brier raw 0.445–0.481,
    Brier cal → 0.2501…0.2505, hit 50.4%; σMAE/√h = const;
  * синтез с настоящим AR-импульсом: Brier cal 0.104 при zero 0.25, hit 85% —
    т.е. харнес навык видит. Отсюда: Brier raw ≈ 0.44 на реальных BTC/ETH — это
    ДИП-СИГНАЛ, а не «перевёрнутый» сигнал (0.44 — ровно то, что даёт ноль).

Мотивация правок (прогон юзера 18.09, BTC+ETH, 720d, n=1035466):
  Brier zero = 0.2500 (как и должно быть), Brier raw = 0.44–0.485 — наивная
  экстраполяция импульса НЕ полезна, она переуверенная; вывод о навыке делает
  только cal-колонка. σMAE раньше считалась с h вместо √h — исправлено.

Бейзлайн-часть: чистый stdlib, идёт на любом t3. TFM-часть требует
`pip install timesfm[torch]` (GPU желателен; CPU ~1s/окно батчами).

Примеры:
  python3 tfmprobe.py --symbol BTCUSDT --days 720 --cache /tmp/kl
  python3 tfmprobe.py --symbol BTCUSDT --days 720 --cache /tmp/kl --model timesfm --batch 256
  python3 tfmprobe.py --gen 40000            # синтетика, прогнать харнес без сети
"""
import argparse
import json
import math
import os
import sys
import time
import urllib.request
from array import array

API = "https://api.binance.com/api/v3/klines"
SQ2 = math.sqrt(2.0)
WARMUP = 1100          # окон не берём, пока EWMA σ не прогрета
# лог-сетка κ: нижняя граница должна быть достаточно мелкой, чтобы при
# отсутствии навыка p схлопывалось к 0.5 (иначе Brier cal > 0.25 вводит в заблуждение)
KGRID = [10.0 ** (-3 + 3.0 * i / 19.0) for i in range(20)]


def Phi(x):
    return 0.5 * (1.0 + math.erf(x / SQ2))


def fetch_series(symbol, days, cache_dir):
    """1m close'ы за days; кэш в cache_dir/<symbol>.jsonl (строк по 1000)."""
    os.makedirs(cache_dir, exist_ok=True)
    fn = os.path.join(cache_dir, f"{symbol}.jsonl")
    if os.path.exists(fn):
        with open(fn) as f:
            rows = [json.loads(ln) for ln in f]
        if rows:
            return [r[1] for r in rows]
    end_ms = int(time.time() * 1000) // 60000 * 60000
    start_ms = end_ms - days * 86400 * 1000
    rows = []
    t = start_ms
    n = 0
    while t < end_ms:
        url = f"{API}?symbol={symbol}&interval=1m&startTime={t}&limit=1000"
        for attempt in range(4):
            try:
                data = json.loads(urllib.request.urlopen(url, timeout=20).read())
                break
            except Exception as e:
                if attempt == 3:
                    print(f"fetch fail на {t}: {e}; что есть — то пишем",
                          file=sys.stderr)
                    data = []
                time.sleep(1.5 * (attempt + 1))
        for k in data:
            rows.append([int(k[0]), float(k[4])])
        t += 1000 * 60000
        n += 1
        if n % 100 == 0:
            print(f"  {symbol}: {n} запросов, {len(rows)} строк", file=sys.stderr)
    with open(fn, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return [r[1] for r in rows]


def synth(n, seed=7):
    """Синтетика для самотестирования харнеса: AR-моментум + кластеринг вола."""
    import random
    random.seed(seed)
    out = [100.0]
    v = 20.0
    lr = 0.0
    for _ in range(n - 1):
        v = min(max(v * math.exp(random.gauss(0, 0.1)), 4.0), 120.0)
        lr = min(max(lr + random.gauss(0, v / 1e4), -0.03), 0.03)
        out.append(out[-1] * math.exp(lr))
    return out


def fit_alpha(ar, sd, lo, hi, getter=None):
    """α*, минимизирующая mean|ar - a*sd| на [lo,hi). Старт от медианы ar/sd,
    затем локальная сетка ±10%. Одинаковая процедура для EWMA и для TFM —
    поэтому G2-отношение сравнивает качество условной σ, а не handicap метрики."""
    rat = []
    for j in range(lo, hi):
        x = sd[j] if getter is None else getter(j)
        if x and x > 1e-9:
            rat.append(ar[j] / x)
    if not rat:
        return 1.0, float("nan")
    rat.sort()
    a0 = rat[len(rat) // 2]
    m = hi - lo
    best = (1e18, a0)
    for t in range(-10, 11):
        a = a0 * (1.0 + 0.01 * t)
        acc = 0.0
        for j in range(lo, hi):
            x = sd[j] if getter is None else getter(j)
            acc += abs(ar[j] - a * (x if x else 0.0))
        if acc < best[0]:
            best = (acc, a)
    return best[1], best[0] / max(m, 1)


def evaluate(closes, horizons, model=None, batch=256, out_csv=""):
    c = [math.log(x) for x in closes]
    n = len(c)
    # EWMA vol (шаг 1м), λ ≈ час полуразпада
    lam = 0.9988
    rets = [c[i] - c[i - 1] for i in range(1, n)]
    ew = [0.0] * n
    var = 25e-8
    for i, r in enumerate(rets):
        var = lam * var + (1 - lam) * r * r
        ew[i + 1] = math.sqrt(max(var, 1e-12))
    preds, tsd = {}, {}
    if model == "timesfm":
        preds, tsd = tfm_forecast(c, horizons, batch)

    # --- pass 1: собрать окна (z, y, |r|) по каждому горизонту ---
    per = {h: dict(z=array("f"), y=array("b"), ar=array("f"), sg=array("b"),
                   sd=array("f"), ix=array("i"))
           for h in horizons}
    imax = n - max(horizons) - 1
    for i in range(WARMUP, imax):
        last = c[i]
        mom = c[i] - c[i - 5]                      # 5-мин импульс
        base = mom / ew[i]                         # z при h=1 (пер-минутный масштаб)
        for h in horizons:
            r = c[i + h] - last
            if not math.isfinite(r):
                continue
            s = ew[i] * math.sqrt(h)
            d = per[h]
            d["z"].append(base * math.sqrt(h))     # z = mom*h/(σ√h)
            d["y"].append(1 if r > 0 else 0)
            d["ar"].append(abs(r) * 1e4)
            d["sg"].append(1 if (r > 0) == (mom > 0) else 0)
            d["sd"].append(ew[i] * math.sqrt(h) * 1e4)
            d["ix"].append(i)

    rep = {}
    for h in horizons:
        d = per[h]
        m = len(d["y"])
        if m == 0:
            continue
        half = m // 2
        z, y = d["z"], d["y"]
        # κ подбираем на первой половине по log-likelihood, метрим на второй
        best_k, best_ll = 1.0, -1e18
        for k in KGRID:
            ll = 0.0
            for j in range(half):
                p = min(max(Phi(k * z[j]), 1e-6), 1 - 1e-6)
                ll += math.log(p) if y[j] else math.log1p(-p)
            if ll > best_ll:
                best_ll, best_k = ll, k
        tp = preds.get(h) or {}
        sp = tsd.get(h) or {}
        sd = d["sd"]
        ae, mae_e = fit_alpha(d["ar"], sd, 0, half)
        at = 1.0
        mae_t_in = float("nan")
        if sp:
            at, mae_t_in = fit_alpha(d["ar"], sd, 0, half,
                                    getter=lambda j: sp.get(d["ix"][j], 0.0))
        acc = dict(bz0=0.0, bzraw=0.0, bzcal=0.0, llcal=0.0, bzt=0.0, llt=0.0,
                   vmae=0.0, vmaet=0.0, vmaec=0.0, vmaetc=0.0, mar=0.0,
                   sdhat=0.0, hit=0.0, n2=0, nt=0)
        for j in range(half, m):
            i = d["ix"][j]
            p0 = 0.5
            praw = min(max(Phi(z[j]), 0.01), 0.99)
            pcl = min(max(Phi(best_k * z[j]), 0.01), 0.99)
            acc["bz0"] += (p0 - y[j]) ** 2
            acc["bzraw"] += (praw - y[j]) ** 2
            acc["bzcal"] += (pcl - y[j]) ** 2
            acc["llcal"] += -(math.log(pcl) if y[j] else math.log1p(-pcl))
            if i in tp:
                pt = min(max(tp[i], 0.01), 0.99)
                acc["bzt"] += (pt - y[j]) ** 2
                acc["llt"] += -(math.log(pt) if y[j] else math.log1p(-pt))
            acc["vmae"] += abs(sd[j] - d["ar"][j])
            acc["vmaec"] += abs(ae * sd[j] - d["ar"][j])
            acc["sdhat"] += sd[j]
            if i in sp:
                acc["vmaet"] += abs(sp[i] * 1e4 - d["ar"][j])
                acc["vmaetc"] += abs(at * sp[i] * 1e4 - d["ar"][j])
                acc["nt"] += 1
            acc["mar"] += d["ar"][j]
            acc["hit"] += d["sg"][j]
            acc["n2"] += 1
        nn = max(acc["n2"], 1)
        rep[h] = dict(kappa=best_k, n=m, n2=acc["n2"],
                      bz0=acc["bz0"] / nn, bzraw=acc["bzraw"] / nn,
                      bzcal=acc["bzcal"] / nn, bzt=acc["bzt"] / nn,
                      llcal=acc["llcal"] / nn, llt=acc["llt"] / nn,
                      vmae=acc["vmae"] / nn, mar=acc["mar"] / nn,
                      vmaec=acc["vmaec"] / nn, sdhat=acc["sdhat"] / nn,
                      alpha_e=ae, alpha_t=at,
                      vmaet=(acc["vmaet"] / acc["nt"]) if acc["nt"] else 0.0,
                      vmaetc=(acc["vmaetc"] / acc["nt"]) if acc["nt"] else 0.0,
                      hit=acc["hit"] / nn)
        per[h] = None                           # отпустить память до следующего h

    if not rep:
        print("нет окон (мало данных?)", file=sys.stderr); return {}
    print(f"\n  P(up) — out-of-sample (вторая половина, n={rep[min(rep)]['n2']})")
    print(f"{'h':>4} {'Brier zero':>10} {'Brier raw':>9} {'Brier cal':>9} "
          f"{'Brier tfm':>9} {'kappa':>9} {'LL cal':>7} {'hit%':>6}")
    for h in sorted(rep):
        r = rep[h]
        bt = f"{r['bzt']:10.4f}" if r["bzt"] else f"{'—':>10}"
        lt = f"{r['llt']:7.4f}" if r["bzt"] else f"{'—':>7}"
        print(f"{h:>4} {r['bz0']:>10.4f} {r['bzraw']:>9.4f} {r['bzcal']:>9.4f} "
              + bt + f" {r['kappa']:>9.4f} {r['llcal']:>7.4f} {100*r['hit']:>6.2f}")
    print(f"\n  vol — σMAE = mean|σ_ewma*sqrt(h) - |r_h|| (б.п.), mean|r_h|")
    print(f"{'h':>4} {'σ̂ ewma':>8} {'σMAE':>8} {'σMAE cal':>9} {'eff':>6} "
          f"{'α*':>6} {'σMAE tfm':>9} {'tfm/ewma':>9} {'mean|r|':>8}")
    for h in sorted(rep):
        r = rep[h]
        eff = r["vmae"] / (0.5354 * r["sdhat"]) if r["sdhat"] else float("nan")
        vt = f"{r['vmaet']:>9.1f}" if r["vmaet"] else f"{'—':>9}"
        rt = f"{r['vmaetc'] / r['vmaec']:>9.3f}" if r["vmaet"] else f"{'—':>9}"
        print(f"{h:>4} {r['sdhat']:>8.1f} {r['vmae']:>8.1f} {r['vmaec']:>9.1f} "
              f"{eff:>6.3f} {r['alpha_e']:>6.3f} " + vt + rt + f" {r['mar']:>8.1f}")
    print("  eff = σMAE/(0.5354·σ̂): 1.000 = EWMA уже на гауссиановском поле;"
          " >1.03 есть реальный запас, который может съесть модель.")
    print("  tfm/ewma = отношение ПОСЛЕ одинаковой α-калибровки обеих сторон (G2).")
    if out_csv:
        with open(out_csv, "w") as f:
            for h in sorted(rep):
                f.write(json.dumps({"h": h, **rep[h]}) + "\n")
    return rep


def tfm_forecast(logc, horizons, batch):
    """Возвращает {h: {i: p_up}} — P(c[i+h] > c[i]) по квантилям TimesFM 2.5.
    Окна контекста 1024, инференс батчами; веса ~800МБ (HF, ~5 мин первая загрузка).
    Замечание: σ_q считаем в тех же log-шагах, что и ретёрны, — единицы согласованы."""
    try:
        import numpy as np
        import torch
        import timesfm
    except ImportError:
        print("нет timesfm/torch: pip install timesfm[torch] — считаю базовые методы",
              file=sys.stderr)
        return {}, {}
    hmax = max(horizons)
    model = timesfm.TimesFM_2p5_200M_torch.from_pretrained(
        "google/timesfm-2.5-200m-pytorch")
    model.compile(timesfm.ForecastConfig(
        max_context=1024, max_horizon=hmax, normalize_inputs=True,
        use_continuous_quantile_head=True, force_flip_invariance=True,
        infer_is_positive=False, fix_quantile_crossing=True))
    n = len(logc)
    idx = list(range(WARMUP, n - hmax - 1))
    out = {h: {} for h in horizons}
    sdc = {h: {} for h in horizons}
    dev = "cuda" if torch.cuda.is_available() else "cpu"
    for s in range(0, len(idx), batch):
        chunk = idx[s:s + batch]
        inputs = [np.array(logc[i - 1024:i], dtype=np.float32) for i in chunk]
        _, q = model.forecast(horizon=hmax, inputs=inputs)   # (B, hmax, Q)
        q = np.asarray(q.cpu() if hasattr(q, "cpu") else q)
        for bi, i in enumerate(chunk):
            for h in horizons:
                qs = q[bi, h - 1]
                lo = float(qs[0]); hi = float(qs[-1])       # P10 и P90
                med = float(qs[len(qs) // 2])
                sd = max((hi - lo) / (2 * 1.2816), 1e-6)
                out[h][i] = Phi((med - logc[i]) / sd)       # P(up) из норм-аппрокс.
                sdc[h][i] = sd                              # прогноз σ на горизонте h
        if s % (batch * 20) == 0:
            print(f"  tfm: {s + len(chunk)}/{len(idx)} окон на {dev}", file=sys.stderr)
    return out, sdc


HLS = (15, 60, 360, 1440)          # полуразпад EWMA², минуты
E_ABS = 1.0 / 0.7979               # E|z| для гауссиана → масштаб к σ
PREF = ("ewma15", "ewma60", "ewma360", "ewma1440", "absw60", "prev", "oracle")


def _ewma_ret(x, hl):
    """EWMA σ (log-шаг) от ряда 1-мин ретёрнов; hl — полуразпад в точках."""
    lam = 0.5 ** (1.0 / hl)
    out = array("f", [0.0]) * (len(x) + 1)
    var = 25e-8
    out[0] = math.sqrt(var)
    for i, v in enumerate(x):
        var = lam * var + (1 - lam) * v * v
        out[i + 1] = math.sqrt(max(var, 1e-18))
    return out


def _ewma_abs(x, hl):
    lam = 0.5 ** (1.0 / hl)
    out = array("f", [0.0]) * (len(x) + 1)
    a = 0.0006 * E_ABS
    out[0] = a
    for i, v in enumerate(x):
        a = lam * a + (1 - lam) * abs(v)
        out[i + 1] = a
    return out


def _solve(A, b):
    """Гауссово решение A x = b (маленькая система, stdlib). None — если вырождено."""
    n = len(b)
    M = [A[i][:] + [b[i]] for i in range(n)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(M[r][col]))
        if abs(M[piv][col]) < 1e-12:
            return None
        M[col], M[piv] = M[piv], M[col]
        pv = M[col][col]
        M[col] = [x / pv for x in M[col]]
        for r in range(n):
            if r != col and M[r][col]:
                f = M[r][col]
                M[r] = [M[r][j] - f * M[col][j] for j in range(n + 1)]
    return [M[i][n] for i in range(n)]


def vol_arena(closes, horizons, out_csv="", step=1):
    """Кто умеет предсказывать размах ОКНА ex ante. Одна процедура на всех:
    α = медиана |r_h|/σ̂ на ПЕРВОЙ половине, MAE на ВТОРОЙ + парная значимость
    против ewma60 (по тем же окнам). 'prev' = realized vol предыдущих h минут,
    'jump' = ×в 30 мин / σ̂ (канал ФОРМЫ хвоста, не масштаба), 'oracle' =
    realized vol самого окна (чит, только как потолок)."""
    from collections import deque
    c = array("d", (math.log(x) for x in closes))
    n = len(c)
    rets = array("d", (c[i] - c[i - 1] for i in range(1, n)))
    ps = array("d", [0.0] * n)                    # ps[k] = Σ_{j<k} rets[j]²
    a = 0.0
    for k in range(1, n):
        a += rets[k - 1] * rets[k - 1]
        ps[k] = a

    def win2(i0_, i1_):                            # Σ r² по минутам [i0, i1)
        return ps[min(max(i1_, 0), n - 1)] - ps[min(max(i0_, 0), n - 1)]

    mx = array("f", [0.0] * n)                     # скользящий max |r| за 30 мин
    dq = deque()
    for k in range(n):
        v = abs(rets[k - 1]) if k > 0 else 0.0
        while dq and dq[-1][1] <= v:
            dq.pop()
        dq.append((k, v))
        while dq[0][0] <= k - 30:
            dq.popleft()
        mx[k] = dq[0][1]

    sig = {f"ewma{hl}": _ewma_ret(rets, hl) for hl in HLS}
    sig["absw60"] = _ewma_abs(rets, 60)
    imax = n - max(horizons) - 1
    half_at = WARMUP + (imax - WARMUP) // 2
    FEAT = ("ewma15", "ewma360", "ewma1440", "prev", "jump")
    rows = []
    for h in horizons:
        col = {k: array("f") for k in PREF}
        col["jump"] = array("f")
        col["har"] = array("f")
        ary = array("f")
        sh = math.sqrt(h) * 1e4
        for i in range(WARMUP, imax, step):
            col["ewma15"].append(sig["ewma15"][i] * sh)
            col["ewma60"].append(sig["ewma60"][i] * sh)
            col["ewma360"].append(sig["ewma360"][i] * sh)
            col["ewma1440"].append(sig["ewma1440"][i] * sh)
            col["absw60"].append(sig["absw60"][i] * sh)
            col["prev"].append(math.sqrt(max(win2(i - h, i), 0.0)) * 1e4)
            col["oracle"].append(math.sqrt(max(win2(i, i + h), 0.0)) * 1e4)
            # jump: σ̂_60 × (max|r|_30м / (σ̂_60·√30)) — «насколько окно хвостатее нормы»
            e60 = max(sig["ewma60"][i], 1e-12)
            col["jump"].append(e60 * sh * min(mx[i - 1] / (e60 * math.sqrt(30.0)), 20.0))
            ary.append(abs(c[i + h] - c[i]) * 1e4)
        ntot = len(ary)
        half = max(ntot // 2, 50)
        keys = list(PREF) + ["jump"]

        def alpha_of(pr):
            rat = sorted(ary[j] / pr[j] for j in range(half) if pr[j] > 1e-9)
            return rat[len(rat) // 2] if rat else float("nan")

        al = {k: alpha_of(col[k]) for k in keys}

        def har_fit(feats, name):
            """OLS по логам на первой половине; предсказание = exp(βx)."""
            p_ = len(feats) + 1
            XtX = [[0.0] * p_ for _ in range(p_)]
            Xty = [0.0] * p_
            used = 0
            for j in range(half):
                xs = [1.0]
                ok = ary[j] > 1e-9
                for k in feats:
                    v = col[k][j]
                    ok = ok and v > 1e-9
                    xs.append(math.log(v) if v > 1e-9 else 0.0)
                if not ok:
                    continue
                y = math.log(ary[j])
                for aa in range(p_):
                    Xty[aa] += xs[aa] * y
                    for bb in range(p_):
                        XtX[aa][bb] += xs[aa] * xs[bb]
                used += 1
            if used <= 20 * p_:
                return
            beta = _solve(XtX, Xty)
            if beta is None:
                return
            pr = array("f", [0.0] * ntot)
            for j in range(ntot):
                lv = beta[0]
                for aa, k in enumerate(feats, start=1):
                    v = col[k][j]
                    lv += beta[aa] * (math.log(v) if v > 1e-9 else 0.0)
                pr[j] = math.exp(min(max(lv, -20.0), 20.0))
            col[name] = pr
            al[name] = alpha_of(pr)
            keys.append(name)

        # har  = + jump (канал формы хвоста);  har0 = без него. Их разница и есть
        # ЗНАЧИМЫЙ вклад хвостов — то, за что вообще стоило платить GPU.
        har_fit(("ewma15", "ewma360", "ewma1440", "prev", "jump"), "har")
        har_fit(("ewma15", "ewma360", "ewma1440", "prev"), "har0")

        err = {}
        for k in keys:
            a = al[k]
            err[k] = array("f", (abs(a * col[k][j] - ary[j]) for j in range(half, ntot)))

        def pair(a, b):
            """Δ и z попарно на одних и тех же окнах второй половины."""
            ea, eb = err[a], err[b]
            m1 = len(ea)
            if m1 < 50:
                return float("nan"), float("nan")
            d = [ea[j] - eb[j] for j in range(m1)]
            mu = sum(d) / m1
            var = max(sum(x * x for x in d) / m1 - mu * mu, 1e-18)
            return mu, mu / math.sqrt(var / m1)

        n2 = max(len(err["ewma60"]), 1)
        mae = {k: sum(err[k]) / n2 for k in keys}
        line = {}
        for k in keys:
            dm, z = pair(k, "ewma60")
            line[k] = (al[k], mae[k], dm, z, 100.0 * dm / mae["ewma60"] if mae["ewma60"] else 0.0)
        if "har" in err and "har0" in err:
            line["__jump_inc__"] = (0.0, 0.0) + pair("har", "har0") + (0.0,)
        rows.append((h, line, n2))
    ks = [k for k in list(PREF) + ["jump", "har", "har0"]]
    print(f"\n  vol-арена: mean|α·σ̂ − |r_h|| (б.п., OOS), ratio к ewma60; шаг {step}")
    print(f"{'h':>4} " + " ".join(f"{k:>10}" for k in ks))
    for h, line, n_ in rows:
        base = line["ewma60"][1]
        print(f"{h:>4} " + " ".join(f"{line[k][1] / base:>10.4f}" if k in line
                                    else f"{'—':>10}" for k in ks))
    print("  парной Δ против ewma60: Δ б.п. / Δ%базы / z   (n≈" +
          ",".join(str(r[2]) for r in rows) + ")")
    dk = [k for k in ks if k != "ewma60"]
    print(f"{'h':>4} " + " ".join(f"{k:>19}" for k in dk))
    for h, line, n_ in rows:
        print(f"{h:>4} " + " ".join(
            f"{line[k][2]:>6.2f} {line[k][4]:>5.2f}% {line[k][3]:>5.1f}"
            if k in line and k != "ewma60" else f"{'—':>19}" for k in dk))
    print("  правило A4': нужно z<=-3 И |Δ%|>=3%.  |z|<3 = не различить.")
    if any("__jump_inc__" in ln for _, ln, _n in rows):
        print("  прирост канала хвостов = har − har0 (тот самый 'quantile head' аргумент):")
        for h, line, n_ in rows:
            j = line.get("__jump_inc__")
            if j:
                print(f"   h={h:<4} Δ={j[2]:>7.3f} б.п.  z={j[3]:>6.1f}")
    if out_csv:
        with open(out_csv, "w") as f:
            for h, line, n_ in rows:
                f.write(json.dumps({"h": h, "n2": n_,
                                    **{k: {"alpha": line[k][0], "mae": line[k][1],
                                           "delta_bps": line[k][2], "z": line[k][3],
                                           "delta_pct": line[k][4]}
                                       for k in line if k != "__jump_inc__"}}) + "\n")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--days", type=int, default=720)
    ap.add_argument("--cache", default="/tmp/kl")
    ap.add_argument("--horizons", default="5,15,60,240")
    ap.add_argument("--model", default="none", choices=("none", "timesfm"))
    ap.add_argument("--batch", type=int, default=256)
    ap.add_argument("--gen", type=int, default=0, help="синтетика вместо сети")
    ap.add_argument("--csv", default="")
    ap.add_argument("--step", type=int, default=1,
                    help="брать каждое N-е окно (дляvol-арены на полном датасете: 7)")
    ap.add_argument("--arena", action="store_true",
                    help="только vol-арена (дешёвые предикторы σ против EWMA), без P(up)")
    a = ap.parse_args()
    hs = [int(x) for x in a.horizons.split(",")]
    if a.gen:
        closes = synth(a.gen)
        print(f"synthetic n={len(closes)}", file=sys.stderr)
    else:
        closes = fetch_series(a.symbol, a.days, a.cache)
        print(f"{a.symbol}: {len(closes)} минут", file=sys.stderr)
    if len(closes) < WARMUP + max(hs) + 50:
        print(f"мало данных (нужно >{WARMUP + max(hs) + 50} минут)", file=sys.stderr)
        sys.exit(2)
    if a.arena:
        vol_arena(closes, hs, out_csv=a.csv, step=max(1, a.step))
    else:
        evaluate(closes, hs, model=a.model if a.model == "timesfm" else None,
                 batch=a.batch, out_csv=a.csv)


if __name__ == "__main__":
    main()
