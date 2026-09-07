# analysis/ — читалка сырых логов kronolog

`kreader.py` (python3.8+, зависимостей нет) читает `.jsonl.gz` из S3 и превращает
raw-вербатим в ряды/метрики. Запускать **из CloudShell** (у роли сервера нет прав
на чтение бакета — так задумано).

## Режимы

```bash
# 0) скачать в CloudShell
curl -sL -o /tmp/kreader.py https://raw.githubusercontent.com/difussion13-netizen/kronos/arena/01a063d9-kronos/analysis/kreader.py
B=kronolog-moi1234

# 1) «что внутри» — скелеты JSON. Начинать отсюда, если парсер чего-то не видит
python3 /tmp/kreader.py probe --bucket $B --day 20260907 --stream rtds --tail 2

# 2) полнота сетки за период (быстро, листинг без скачивания)
python3 /tmp/kreader.py stats --bucket $B --days 20260903-20260907

# 3) лаг Chainlink-фида относительно Binance (главная метрика этапа A)
python3 /tmp/kreader.py lag --bucket $B --day 20260907 --sym btc/usd --files 24 --thresh 5

# 4) пересборка минутных свечей из наших записей
python3 /tmp/kreader.py candles --bucket $B --day 20260907 --sym btcusdt --files 24   # из bookTicker mid
python3 /tmp/kreader.py candles --bucket $B --day 20260907 --tokens <asset_id,..>     # из CLOB last_trade

# 5) сверка наших свечей с официальными klines Binance
python3 /tmp/kreader.py verify --bucket $B --sym BTCUSDT
```

## Что означают метрики `lag`

- `возраст цены в chainlink-сообщении` — насколько «вчерашняя» цена внутри самого
  Chainlink-обновления (timestamp события vs timestamp сообщения).
- `|chainlink − mid(binance в метку)|` — шум/аппроксимация самого оракула.
- `|... в момент приёма|` — ошибка, с которой рынок резолвится фактически — на неё
  и опираемся при моделировании.
- `время доезда` — медиана/p90 задержки донесения движения цены >порога (б.п.).

Интерпретация: если «доезд» стабильно единицы секунд, а ошибка приёма <5–10 б.п. —
оркул не «скрывает» от нас информацию быстрее, чем мы её получаем; это вход в
вопрос «есть ли эдж у модели».

## Формат данных (для своих скриптов)

- строка = `{"t": recv_ns, "raw": <оригинал биржи>}` (или `"text"` для не-JSON кадров);
- binance raw: `{"stream":"btcusdt@bookTicker","data":{b,a,E,...}}`;
- rtds raw: `{"topic":"crypto_prices_chainlink","type":"update","payload":{"symbol","price","timestamp"}}`
  (точные поля уточнить `probe`);
- clob raw: список событий `{"event_type": book|price_change|last_trade, ...}`;
- имена файлов: `<stream>_<YYYYMMDD>_<HHMMSS>.jsonl.gz` — время старта части; у clob
  части режутся ещё и по смене окна (меньше 15 мин) — это норма.

## Ограничения

- CloudShell «живёт» до 50 минут сессии; большие прогоны (`lag --files 96`) считать
  частями или на своём сервере после `sudo pip install boto3`… но там нет прав на
  чтение бакета ролью — проще разбить `--files`.
- `verify` обращается к api.binance.com — из некоторых регионов AWS сети Binance
  фильтруются; это не дефект данных.

## dataset.py + smoke.py — датасет и «палочка-выручалочка»

Сборка таблицы «бары + фичи (только прошлое) + метки future» и честная
walk-forward оценка логистической регрессией. Оба файла скачивать в одну папку
(dataset импортирует kreader).

```bash
D=/tmp; for f in kreader dataset smoke; do curl -sL -o $D/$f.py \
  https://raw.githubusercontent.com/difussion13-netizen/kronos/arena/01a063d9-kronos/analysis/$f.py; done
python3 $D/dataset.py --bucket $B --days 20260903-20260909 --tf 300 \
    --assets btc,eth,sol,xrp --outdir /tmp/ds
python3 $D/smoke.py --dir /tmp/ds --asset btc --horizon 5
```

- `dataset` читает binance+rtds (не clob! он тяжёлый) → csv-таблицы + meta.json
  (описание колонок) + npz если есть numpy. Выводит баланс классов: доля `flat`
  при пороге 4.7 б.п. — это доля рынка, где «торговать нечего» в принципе.
- `smoke`: train/holdout 70/30 по ВРЕМЕНИ; метрики acc/balanced/AUC/Brier/точность
  топ-10% самых уверенных; два базлайна (моментум, мажоритарный класс); вердикт.
- Честные ориентиры: неделя данных → acc ≈ 50–51% — это НОРМА («сигнала нет»).
  Устойчивые 52–55% на длинном периоде — повод строить нормальную модель.
  >56% на неделе — сначала ищи утечку будущего в фичи, потом радуйся.
- Прогон занятой: 7 суток ≈ 3–5 минут в CloudShell (clob не читается!).
