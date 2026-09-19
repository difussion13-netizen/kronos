# Полимаркет → Binance: исследования информационного края

После закрытия MM и тейкера на Polymarket (docs/mm-verdict-20260920.md) вопрос
перевёрнут: Polymarket — не площадка для торговли, а **источник информации**.
Его цены агрегируют informed flow, а структура резолва (oracle-lag ~30 сек)
создаёт временное окно, в котором спот уже знает направление, а рынок ещё нет.

Цель: найти tradeable edge на Binance spot/perp, используя Polymarket-данные
как сигнал. Все эксперименты — офлайн на собранных датасетах, без торговли.

---

## Данные и форматы (не гадаем — сверяем с кодом)

### minutes_YYYYMMDD.csv (из clobwin.py --minutes)
```
columns: day, minute, asset, close
day     = "20260908" (строка)
minute  = unix timestamp начала минуты (int, НЕ 0..1439!) — каждая следующая +60
asset   = "btc" / "eth" / "sol" / "xrp"
close   = цена закрытия минуты (float, aggTrade last price)
```
**ВАЖНО:** `minute` — это unix timestamp (например 1788825600 = 2026-09-08 00:00:00 UTC),
а НЕ номер минуты в сутках. Ключевание: `{asset: {unix_ts: close}}`.
Источник: `clobwin.py:do_minutes()` — aggTrade из `binance/` стрима, агрегация
последней сделки в минуте. Файл: `$WORK/win/minutes_YYYYMMDD.csv`.

### windows_YYYYMMDD.csv (из clobwin.py)
```
columns: slug, asset, code, start, end, y, b240, a240, b120, a120, b60, a60,
         b30, a30, b15, a15, b5, a5, b0, a0, bs240..bs0, as240..as0,
         d2b0..d2a0, agg_n, agg_vol
```
- `start`/`end` = unix timestamp границ окна (start = начало, end = start + 300 для 5m)
- `y` = 1 если close(end-1) >= close(start) (наш прокси Up)
- `b{cp}`/`a{cp}` = bid/ask на чекпоинте за cp секунд до end
- `d2b{cp}`/`d2a{cp}` = объём в пределах 2¢ от mid (depth)
- `agg_n`/`agg_vol` = счётчики aggTrade-потока за окно

### dataset.py output (feature table для ML)
```
columns: t0, r1, r3, r12, volat12, range_bps, vwap_dev_bps,
         cl_off_bps, cl_age_s, n_trades, hour_sin, hour_cos, close,
         move_bps_5m, dir_5m, cls_5m
```
- `t0` = unix timestamp ЗАКРЫТИЯ бара (все фичи по данным <= t0)
- `r1/r3/r12` = доходность за 1/3/12 баров, б.п.
- `volat12` = std 1-баровых доходностей (×12 баров), б.п.
- `range_bps` = (high−low)/close бара, б.п.
- `vwap_dev_bps` = close/vwap внутри бара, б.п.
- `cl_off_bps` = Chainlink vs close, б.п.
- `cl_age_s` = возраст CL-цены, сек
- `n_trades` = log1p(число тиков)
- `close` = цена закрытия бара
- `move_bps_5m` = будущее движение (МЕТКА, не фича!)
- `dir_5m` = 1/−1 (МЕТКА)
- `cls_5m` = 1/0/−1 с порогом flat (МЕТКА)
Файл: `$DS/{asset}_300s.csv` + `.npz`.
Генератор: `python3 dataset.py --bucket B --days ... --tf 300 --outdir /tmp/ds`

### tokens_map.json
```
{token_id: {"slug": "btc-updown-5m-...", "asset": "btc", "code": "5m",
            "start": epoch, "up": true/false}, ...}
```
Привязка токенов к окнам. Поле `start` = unix timestamp начала окна.

### Покрытие (факт, 20.09):
- 04–06: btc only (~1443 мин/сутки)
- 07: btc 1413, eth/sol/xrp 136 (частичные)
- 08–13: все 4 актива ~1443/сутки (полные)
- 14: 1083 (обрезаны)
- 15–16: битые
- 17: 1442 (полные)
- 18: 1201 (обрезаны)

---

## Идеи (по приоритету)

### R1. Trajectory Features — «форма первых 3 минут»
**Оценка: 7/10. Данные: ✅ минутки. Сложность: низкая.**

Гипотеза: паттерн движения цены в первые N минут 5-минутного окна предсказывает
направление финальной минуты.

**Формулировка для ML:**
- Вход: 3 минутные close-цены [m, m+1, m+2] (первые 3 минуты окна)
- Таргет: `y = 1 if close[m+4] > close[m] else 0` (рост к концу окна)
- Фичи:
  ```
  ret_1 = (close[m+1] − close[m]) / close[m] * 1e4     # б.п.
  ret_2 = (close[m+2] − close[m+1]) / close[m+1] * 1e4
  ret_3 = (close[m+2] − close[m]) / close[m] * 1e4      # momentum
  range_3 = (max(c[m..m+2]) − min(c[m..m+2])) / close[m] * 1e4
  reversal = 1 if sign(ret_1) != sign(ret_2 − ret_1) else 0
  accel = (ret_2 − ret_1)                                 # ускорение
  skew = (close[m+2] − mean(c[m..m+2])) / (range_3 + 1e-9)
  ```
- Модель: LightGBM (200 деревьев, max_depth=5, min_data=50)
- Валидация: train 04–13, test 14/17/18 (time-series split, НЕ random)

**Пороги (предрешены):**
- accuracy > 55% при n_test > 500 → зелёный (есть edge, строим фильтр)
- accuracy 52–55% → жёлтый (нужен фильтр P>0.65, проверяем calibration)
- accuracy ≤ 52% → красный (шум)

**Данные:** минутки `$WORK/win/minutes_*.csv`. Окна — из `tokens_map.json`
(поле `start` + code → границы). Сутки 08–13 для train, 14+17+18 для test.

**Реализация:** `analysis/trajectory.py` — standalone скрипт, selftest на
синтетике (тренды up/down/flat → проверяем, что модель их ловит).

---

### R2. Cross-Asset Lead-Lag — «BTC лидирует альткоины»
**Оценка: 7/10. Данные: ✅ минутки (08–13, 6 полных суток). Сложность: низкая.**

Гипотеза: BTC движется первым → ETH/SOL/XRP следуют с лагом 1–3 минуты.

**Формулировка:**
- На каждой минуте: `btc_ret[t] = (btc_close[t] − btc_close[t−1]) / btc_close[t−1]`
- Корреляция: `corr(btc_ret[t], alt_ret[t+k])` для k=1,2,3,5
- Порог: r > 0.3 и p < 0.01 на ≥2 из 3 альтов → сигнал robust

**Применение:**
- Если BTC растёт на >X б.п. за 1 минуту → long ETH/SOL/XRP на t+1..t+3
- Стоп: trailing или fixed на Y б.п.
- EV = средний выигрыш × win_rate − средний проигрыш × (1−win_rate) − фи

**Данные:** те же минутки, пересечение суток 08–13 (все 4 актива есть).
**Реализация:** добавить в `analysis/trajectory.py` (или отдельный `leadlag.py`).

---

### R3. Path-Dependent Volatility — «форма предсказывает волатильность»
**Оценка: 6/10. Данные: ✅ минутки. Сложность: низкая.**

Гипотеза: форма траектории в первые 3 минуты предсказывает **размах** (не
направление) последней минуты. Полезно как risk management фильтр.

**Формулировка:**
- Таргет: `vol_last = |close[m+4] − close[m+3]| / close[m+3] * 1e4` (б.п.)
- Фичи: те же, что R1 (range_3, skew, accel, ret_3)
- Модель: Ridge/Lasso регрессия (простая, interpretable)
- Метрика: R² > 0.05 и корреляция pred/actual > 0.25 → сигнал

**Применение:**
- predicted_vol > threshold → не scalp (too risky)
- predicted_vol < threshold → scalp с tight stop
- Или: predicted_vol high → long vol на perp (straddle)

**Реализация:** в `trajectory.py`, параллельно с R1.

---

### R4. MMP Probability Momentum (требует live feed)
**Оценка: 6/10. Данные: ⚠️ нужен Polymarket price feed. Сложность: средняя.**

Гипотеза: динамика Polymarket-цены в первые 30–90 секунд окна = informed flow
→ сигнал для Binance scalp.

**Формулировка:**
- Наблюдаем MMP midpoint на t=0..90 сек
- `delta_p = P(up, t=90) − P(up, t=0)`
- Если `|delta_p| > 0.15` за <60 сек → рынок «знает»
- На Binance: scalp в направлении delta_p

**Данные:** НЕТ в текущем датасете. Нужен:
- Вариант A: исторические Polymarket-цены (API /ws-live-data)
- Вариант B: реконструкция из CLOB-книг (bid/ask midpoint на старте окна)
  — частично есть в `windows_*.csv` (поля b60/a60 на cp=60)

**Реализация:** после R1/R2, если они покажут edge. Сначала проверяем на
книгах (b60/a60 как прокси P(up)), потом, если promising — live feed.

---

### R5. Implied vs Realized Vol (требует MMP feed)
**Оценка: 6/10. Данные: ⚠️ как R4. Сложность: средняя.**

Гипотеза: расхождение implied vol (из MMP-цены) и realized vol (из Binance)
предсказывает краткосрочное движение.

**Формулировка:**
```
p_up = MMP_midpoint на старте окна
implied_σ = inverse_normCDF(p_up) × spot / √(5 мин)
realized_σ = std(returns, last 30 мин)
spread = implied_σ − realized_σ
```
- spread >> 0 → mean reversion (fade)
- spread << 0 → breakout (follow)

**Данные:** как R4. Плюс 30-минутная история realized vol (из минуток).

---

### R7. Learned P(up) Model (замена Gaussian Φ)
**Оценка: 8/10. Данные: ✅ venue.jsonl + минутки. Сложность: средняя.**

Гипотеза: параметрическая Φ(Δ/(σ√τ)) уступает модели, обученной на реальных
платежах площадки. Преимущества: fat tails, асимметрия, regime switching,
книга, время суток.

**Данные:** venue.jsonl ({asset, start, y, ptb, fp}) + минутки → фичи в момент
открытия окна. Метка: y ∈ {0,1} по venue (priceToBeat/finalPrice).

**Модель:** Logistic regression (fallback) / LightGBM (если есть).
**Порог:** Brier < 0.24 (лучше Gaussian) → ЗЕЛЁНЫЙ.
**Реализация:** `analysis/price_model.py` (--selftest OK).

### R6. Opening Microstructure
**Оценка: 5/10. Данные: ✅ минутки. Сложность: низкая.**

Гипотеза: объём и направление начального импульса (первые 1–2 минуты)
предсказывает исход окна.

**Формулировка:**
- `initial_dir = sign(close[m+1] − close[m])`
- `initial_vol = volume[m] + volume[m+1]` (если есть agg_vol)
- Если initial_dir=up и initial_vol > 1.5× median → P(close>open) > 60%?

**Замечание:** taker-зонд уже проверял похожее (cp=0, Δ≥15, n=20). На
минутках сигнал может быть слишком редким, но **на тиках** (aggTrade с
resolution < 1 сек) — возможно robust. Это путь к R1 на тиках, не отдельная
идея.

---

## Порядок реализации

```
Phase 1 (час работы, данные уже есть):
  R1 → trajectory.py --selftest
  R2 → встроить в trajectory.py или отдельный скрипт
  R3 → параллельно с R1

Phase 2 (если Phase 1 зелёный):
  R4/R5 → нужен Polymarket price feed (отдельная задача)
  Ensemble → комбинация R1+R2+R4

Phase 3 (если Phase 2 зелёный):
  Binance paper trading → отдельный модуль
  (не раньше, чем edge подтверждён на OOS)
```

## Пороги решения (предрешены)

| Критерий | Зелёный | Жёлтый | Красный |
|---|---|---|---|
| R1 accuracy OOS | >55% n>500 | 52–55% | ≤52% |
| R2 correlation | r>0.3, p<0.01, ≥2 alt | r>0.2 | r≤0.2 |
| R3 R² | >0.05 | 0.02–0.05 | <0.02 |
| R4 signal rate | >5/день | 1–5/день | <1/день |

Все красные → направление «Polymarket → Binance» закрывается протоколом.
Хотя бы один зелёный → строим trading module.

---

## Файлы-источники
- Минутки: `$WORK/win/minutes_YYYYMMDD.csv`
- Окна: `$WORK/win/windows_YYYYMMDD.csv`
- Карта токенов: `$WORK/win/tokens_map.json`
- dataset.py output: `$DS/{asset}_300s.csv` (генерируется на боксе)
- Протокол закрытия: `docs/mm-verdict-20260920.md`