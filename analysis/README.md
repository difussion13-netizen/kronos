# analysis — инструменты над S3-логами kronolog

Инструменты читают логи `s3://BUCKET/kronolog/<stream>/<YYYYMMDD>/<stream>_<YYYYMMDD>_<HHMMSS>.jsonl.gz`
через AWS CLI (`aws s3 cp` / `aws s3 sync` — работают из CloudShell и с сервера).

## kreader.py — читалка и диагностика

Режимы: `probe` (рентген строк потока), `stats` (полнота сетки файлов по дням:
96/96 частей для ротации 15м = полный день; «недобор» = дыра), `lag` (задержка/смещение
Chainlink против binance-миды), `candles` (сборка свечей из логов), `verify`
(наши минуты против ОФИЦИАЛЬНЫХ klines Binance: два среза — mid vs (high+low)/2 и
close-vs-close; медиана close-vs-close ≈ 0 при полном совпадении, >2 б.п. = тики опаздывают).

Ключи: `--bucket B [--prefix kronolog] --day|--days A-B [--files N|0] [--stream s] [--sym s] [--tf s]`.
С каталога `--cache DIR` (переменная окружения `KRONOS_CACHE`) читает локально, без CLI на строку.

## dataset.py — датасет для модели

Тики binance + Chainlink (rtds) → 1м-бары → `--tf`-секундные бары → фичи строго
point-in-time (r1/r3/r12, volat12, range_bps, vwap_dev, cl_off/age, n_trades, час sin/cos)
→ метки движения за `--horizons` минут с порогом flat `--floor-bps` (по умолчанию 4.7 б.п.
= замеренный пол шума оракула, см. docs/measured-oracle-latency.md). clob НЕ читается.
Выход: `{asset}_{tf}s.csv` + `meta.json`. Кэш загрузки из S3: `--cache DIR` (aws s3 sync
по дням×потокам, маркер .done; пустые дни отметки не получают и докачиваются позже).

## smoke.py — есть ли вообще сигнал

Walk-forward 70/30 по ВРЕМЕНИ: логистическая регрессия (чистый python) против базлайнов
моментума r1 и мажоритарного класса. Выводит acc/balanced-acc/AUC/Brier/acc-верхней-декады,
bootstrap по дневным блокам (учитывает перекрытые метки), вердикт по правилам:
~50% = сигнала нет; 52–55% устойчиво = сигнал; >56% = искать утечку.

Флаги: `--drop ф1,ф2` — ablation (префикс снимает семейство: `--drop hour` убирает hour_sin+cos);
`--wfa` — по дням: для каждых UTC-суток модель учится строго на данных до них; вердикт wfa:
устойчиво, если ≥4 дней и mean−sd > 0.5. `--min-rows` — порог полезности выборки.

Прогон на одной неделе (btc, 60м) 2026-09-08: acc 0.592, 3/3 дня wfa выше 0.5, ablation
показала источник в инерции/активности (r*/volat/n_trades), а не в cl_*/vwap/hour — но 3 дня
это разведка, не вердикт.

## Типовой цикл проверки качества

1. `kreader stats --days A-B` — дыр в сетке нет (допустимы в дни рестартов службы);
2. `kreader verify --day D --sym btcusdt` — close-vs-close медиана ≲1 б.п.;
3. `kreader lag --days D --sym btc/usd` — возраст CL не уехал за минуту;
4. `dataset.py --cache DIR ...` — у всех активов строки с метками, доли flat 55–62%;
5. `smoke --wfa` по горизонтам/активам — копить ≥10 тестовых дней до решения о модельном этапе.
