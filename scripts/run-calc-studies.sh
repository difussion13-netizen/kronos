#!/usr/bin/env bash
# run-calc-studies.sh — один запуск на калькуляторе: датасет книг -> тейкер-исследование
# -> MM-сим (гейтная конфигурация и прогон ёмкости). Идемпотентен: каждый шаг помечен
# файлом-маркером, повторный запуск досчитывает только то, что не готово.
#
#   sudo bash scripts/run-calc-studies.sh                     # всё
#   STEPS=taker_min,taker_1s bash run-calc-studies.sh         # только тейкер
#   STEPS=mm_gate,mm_capacity bash run-calc-studies.sh         # только MM
#   nohup bash run-calc-studies.sh > /tmp/run.log 2>&1 &      # как принято на боксе
#
# Шаги: disk, get, sync, days, clobwin, venue, taker_min, taker_1s, mm_gate, mm_capacity.
# sync = взять готовый датасет из s3://…/kronos/win/; clobwin строит только то, чего нет.
# Ничего не выкладывает, никуда не пишет кроме $WORK; торговый код не запускает.
set -uo pipefail

WORK=${WORK:-/tmp/calc}
BUCKET=${BUCKET:-kronolog-moi1234}
MAPKEY=${MAPKEY:-kronos/win/tokens_map.json}
DAYS=${DAYS:-auto}                       # auto = сутки, где реально есть clob-поток
ASSETS=${ASSETS:-btc,eth,sol,xrp}
JOBS=${JOBS:-6}
INV_USD=${INV_USD:-500}
MAXPART=${MAXPART:-0.05}
MAXW=${MAXW:-20000}                      # потолок окон для докачки 1с-клинов
MINUTES=${MINUTES:-1}                     # 0 = без бинарных минут (см. need_min)
RESCAN=${RESCAN:-0}                       # 1 = пересобрать ids и карту токенов с нуля
RATE=${RATE:-45}
REPO=${REPO:-https://raw.githubusercontent.com/difussion13-netizen/kronos/arena/01a063d9-kronos}
BRANCHQ=${BRANCHQ:-nc=studies1}
S=${STEPS:-disk,get,sync,days,clobwin,venue,taker_min,taker_1s,mm_gate,mm_capacity}
LOG=$WORK/logs
mkdir -p "$WORK" "$LOG"

have() { [[ ",$S," == *",$1,"* ]]; }
say() { printf '\n=== %s — %s\n' "$(date -u +%H:%M:%S)" "$1"; }
ok()  { : > "$WORK/.done_$1"; echo "   готово ($1), $(date -u +%H:%M:%S)"; }
st()  { [[ -f "$WORK/.done_$1" ]] && echo "   уже готово, пропуск ($1)" && return 1; return 0; }
# binance/minutes нужны не всем шагам: у 1с-тейкера источник — клины Binance, а у
# минутной решётки и MM-сима — bench=close(минута). MINUTES=0 означает «минут в
# бакете под наш список нет»: тогда считаются только 1с-шаги, и это честно, потому
# что бакет несёт минуты с 14.09 (за 04–13 их надо сначала залить с бокса).
need_min() { [[ "$MINUTES" == "0" ]] && echo "   $1 пропущен: MINUTES=0 (нет binance/minutes)" && return 1; return 0; }
die() { echo "СТОП: $1" >&2; exit 2; }

# Один прогон на каталог. Маркеры (.done_*) и логи общие, поэтому два параллельных
# запуска на одном WORK = молчаливая гонка: 20.09 второй прогон успел переставить
# .done_clobwin, пока первый был на taker_min, и оба дописывали один kl1s.jsonl.
# Отличить потом это от «данные такие» невозможно, поэтому блокируем прямо здесь.
if command -v flock >/dev/null 2>&1; then
  exec 9>"$WORK/.lock" || die "не могу открыть лок в $WORK (права/место?)"
  flock -n 9 || die "в $WORK уже идёт другой прогон (pid $(cat "$WORK/.pid" 2>/dev/null)); дождись его или разнеси каталоги: WORK=/tmp/calc2 …"
else
  # flock — util-linux, обычно есть; если его нет, проверка слабее (по pid), но и она
  # лучше гонки: хуже «не запустилось» в одном случае, чем испорченный датасет всегда.
  OTHER=$(cat "$WORK/.pid" 2>/dev/null || true)
  [[ -n "$OTHER" ]] && kill -0 "$OTHER" 2>/dev/null && die "в $WORK уже идёт прогон (pid $OTHER, flock недоступен)"
fi
echo $$ > "$WORK/.pid"


# ---------------------------------------------------------------- 0. disk
if have disk; then
  say "диск и окружение"
  FREE_KB=$(df -Pk "$WORK" | awk 'NR==2{print $4}')
  [[ -n "$FREE_KB" && "$FREE_KB" -lt 1048576 ]] && die "на $WORK свободно ${FREE_KB}K (<1ГБ): раздаточный каталог + 1с-кэш требуют ~1ГБ"
  echo "   свободно на $WORK: $((FREE_KB / 1024)) МБ; ядер: $(nproc)"
  command -v python3 >/dev/null || die "нет python3"
  command -v aws >/dev/null || die "нет aws cli (AL2023 должен нести его из коробки)"
  # Защита от «прогнали не там»: на боксе-логгере живут чужие данные и чужие
  # обязательства (прод = только логгер), поэтому сюда не лезем вообще.
  [[ -f /opt/kronolog/app/config.yaml || -d /opt/kronolog ]] && die "это бокс-логгер (/opt/kronolog есть); перенеси прогон на калькулятор"
  aws sts get-caller-identity >/dev/null 2>&1 || die "aws не отдаёт identity: на SSM-сессии надо unset AWS_PROFILE/ключи"
  ok disk
fi

# ---------------------------------------------------------------- 1. get
if have get; then
  say "скрипты из репо (ветка arena/01a063d9-kronos)"
  for f in clobwin markcalc takerprobe clobmm; do
    curl -fsSL -o "$WORK/$f.py" "$REPO/analysis/$f.py?$BRANCHQ" || die "не скачал $f"
    python3 -m py_compile "$WORK/$f.py" || die "$f не компилируется"
  done
  python3 "$WORK/takerprobe.py" --selftest || die "takerprobe --selftest упал"
  ok get
fi

# --------------------------------------------------------------- 1b. sync
# Датасет «окно -> книга» стоит 5–15 минут CPU-часа на сутки и 8.8 ГБ стрима, а
# готовый (03–14.09) лежит в том же бакете рядом: kronos/win/windows_*.csv. Поэтому
# сначала пробуем взять его, и только чего в нём нет — строим. Помечать clobwin
# готовым при этом честно: шаг «обеспечить датасет», а не «вызвать clobwin.py».
days_from_catalog() {
  ls "$WORK"/win/windows_*.csv 2>/dev/null | grep -oE '[0-9]{8}' | sort -u | paste -sd, -
}
if have sync && st sync; then
  say "готовый датасет из s3://$BUCKET/kronos/win/ (строить нужно только то, чего там нет)"
  mkdir -p "$WORK/win"
  aws s3 cp "s3://$BUCKET/kronos/win/" "$WORK/win/" --recursive --exclude '*' \
      --include 'windows_*.csv' --include 'minutes_*.csv' --only-show-errors \
      > "$LOG/sync.log" 2>&1 || echo "   скачивание вернуло ненулевой код (см. $LOG/sync.log) — дальше смотрим, что на диске"
  F=$(ls "$WORK"/win/windows_*.csv 2>/dev/null | head -1)
  if [[ -z "$F" ]]; then
    echo "   в kronos/win/ нет windows_*.csv -> датасет строим шагом clobwin"
  else
    H=$(head -1 "$F")
    # Обязательны ровно те колонки, которые ЧИТАТЕЛИ берут без .get()-фолбэка:
    # clobmm и takerprobe меряют цены по b{cp}/a{cp} (both use r.get for the rest).
    # Первый вариант проверки требовал ещё bs240/as240 — и отверг пригодный снапшот.
    MISS=""
    hascol() { [[ ",$H," == *",$1,"* ]]; }
    for col in start end asset code b240 a240; do
      hascol "$col" || MISS="$MISS $col"
    done
    [[ -n "$MISS" ]] && die "в снапшоте нет обязательных колонок:$MISS — по такому каталогу считать нельзя; пересобери: rm -f $WORK/.done_clobwin && STEPS=clobwin RESCAN=1 bash $0"
    if hascol vol_usd; then
      echo "   vol_usd есть: фильтр участия (--max-part) работает"
    else
      # «ёмкость 0» и «нет данных об обороте» в таблице неотличимы, а разница — вся
      # интерпретация: без vol_usd каждый окном считается «тонким» и все заявки
      # отбрасываются. Поэтому фильтр ВЫКЛЮЧАЕКСЯ вслух, а не молча даёт нули.
      MAXPART=0
      echo "   ВНИМАНИЕ: vol_usd нет -> --max-part 0 (фильтр участия выключен),"
      echo "   иначе ёмкость выглядела бы нулём там, где просто нет колонки оборота"
    fi
    hascol bs240 || echo "   размеров топа (bs/as) нет — справочно: сим их не читает"
    if [[ "$H" == *,d2b240,* ]]; then
      echo "   колонки ёмкости d2b/d2a есть (датасет построен после 19.09)"
    else
      echo "   колонок d2b/d2a НЕТ (этот каталог построен НЕ текущим clobwin): «по объёму»"
      echo "   в тейкере будет н/д; MM-симу это не мешает (участие он меряет по vol_usd),"
      echo "   а ёмкость по глубине стакана требует пересборки суток с --depth-cents"
    fi
    # Те же «50 строк на сутки», что и после стройки: снапшот может лежать в бакете
    # обрезанным (его тоже кто-то писал в спешке), и тогда «датасет есть» снова
    # означало бы «считаем по пустоте».
    # Короткие сутки = либо обрезанная запись, либо мусор от неудачной стройки на ЭТОМ
    # боксе (20.09: 17/18.09 остались файлами из одной шапки от прогона с пустой трубой).
    # Auto-удалять данные нельзя, поэтому имя файла называется дословно и вместе с
    # парным minutes_*, а решение остаётся за человеком.
    for f in "$WORK"/win/windows_*.csv; do
      N=$(grep -c . "$f" 2>/dev/null || echo 0)
      [[ "$N" -lt 50 ]] && die "сутки $(basename "$f"): $N строк — пусты или обрезаны; удали их вместе с парой: rm -f $f ${f/windows_/minutes_} и, если это нужные сутки, построй (STEPS=clobwin RESCAN=1)"
    done
    # Неполные сутки (14.09 строился до 18:24 -> 1177 строк против ~1565) честны как
    # выборка, но врут в per-day метрике MM: «дней в плюсе» считает их полными.
    CNT=$(for f in "$WORK"/win/windows_*.csv; do printf '%s\n' "$(grep -c . "$f" 2>/dev/null || echo 0)"; done | sort -n)
    MAX=$(printf '%s
' "$CNT" | tail -1); SHORT=""
    for f in "$WORK"/win/windows_*.csv; do
      N=$(grep -c . "$f"); [[ "$N" -lt $(( MAX * 8 / 10 )) ]] && SHORT="$SHORT $(basename "$f" .csv)($N)"
    done
    [[ -n "$SHORT" ]] && echo "   ВНИМАНИЕ, неполные сутки:$SHORT (макс $MAX строк) — в per-day" \
                       && echo "   метрике MM они считаются полными; помни это у гейта «P&L/день»"
    echo "   строк по суткам: $(for f in "$WORK"/win/windows_*.csv; do printf '%s ' "$(grep -c . "$f")"; done)"
    NM=$(ls "$WORK"/win/minutes_*.csv 2>/dev/null | grep -cE '.')
    echo "   минуты: $NM суток из $(days_from_catalog | tr ',' '\n' | grep -c .) (MM без минуток не считается)"
    echo "   сутки каталога: $(days_from_catalog)"
    ok clobwin
  fi
  ok sync
fi

# ---------------------------------------------------------------- 2. days
if have days; then
  say "какие сутки пригодны (книги CLOB — жёсткое условие тейкер-исследования)"
  if [[ "$DAYS" == "auto" && -n "$(days_from_catalog)" ]]; then
    # Брать надо из НЕГО, а не из «разума и binance/minutes»: пол по минутам резал
    # выборку до 3 суток, хотя 12 уже лежали в бакете.
    DAYS=$(days_from_catalog)
    echo "   датасет на месте -> сутки берём из каталога: $DAYS"
    echo "$DAYS" > "$WORK/.days"
    ok days
  else
  HAVECLOB=$(aws s3 ls "s3://$BUCKET/kronolog/clob/" --recursive 2>/dev/null \
             | grep -oE '202[0-9]{5}' | sort -u | tr '\n' ' ')
  [[ -z "${HAVECLOB// /}" ]] && die "в $BUCKET/kronolog/clob/ нет ни одних суток — строить датасет не из чего"
  echo "   clob есть за: $HAVECLOB"
  if [[ "$DAYS" == "auto" ]]; then
    # рваные сутки 15–16.09 отброшены руками (не «размахом с дыркой»): список
    # передаётся дальше дословно, и число суток видно в каждом шаге.
    BEST=""
    FLOOR=20260914
    [[ "$MINUTES" == "0" ]] && FLOOR=20260903      # минуты не нужны -> пол не нужен
    for d in $HAVECLOB; do
      [[ "$d" < "$FLOOR" ]] && continue
      case "$d" in 20260915|20260916) continue ;; esac
      BEST+="$d,"
    done
    DAYS=${BEST%,}
    [[ -z "$DAYS" ]] && die "после фильтра (>=$FLOOR, кроме 15–16) не осталось суток"
  fi
  echo "$DAYS" > "$WORK/.days"
  echo "   беру: $DAYS"
  ok days
  fi
fi
DAYS=${DAYS:-$(cat "$WORK/.days" 2>/dev/null || echo "")}
[[ -n "$DAYS" ]] || die "DAYS пуст (шаг days не выполнен?)"
# «auto» — директива шага days, а не сутки. Если он дожил до clobwin, значит days не
# отработал (или .days записан до фильтра), и дальше поехал бы файл windows_auto.csv.
[[ "$DAYS" == *auto* ]] && die "DAYS=$DAYS: шаг days не отработал — запусти STEPS=days (или задай список суток явно)"

# ---------------------------------------------------------------- 3. clobwin
if have clobwin && st clobwin; then
  say "датасет «окно -> книга» (+минутки): $DAYS, $JOBS суток параллельно"
  echo "   стрим 8.8ГБ/сутки идёт трубой, на диск пишется ~0.6МБ/сутки"
  # Шаг пишет в свой лог и молчит 10–25 минут, а самая нужная строка — первая:
  # «токенов N». Поэтому говорим, куда смотреть, и повторим итог в stdout потом.
  echo "   прогресс в другой терминал: tail -f $LOG/clobwin.log   (ждать «passA … токенов N»; N=0 = читаемость/формат)"
  MINFLAG=(--minutes); [[ "$MINUTES" == "0" ]] && MINFLAG=()
  # --rescan нужен, когда карта/покрытие врёт: .days-ledger скачан из S3 чужим боксом,
  # и он говорит «сутки покрыты», хотя на этом боксе ids не собирались.
  RSFLAG=(); [[ "$RESCAN" == "1" ]] && RSFLAG=(--rescan)
  python3 "$WORK/clobwin.py" --bucket "$BUCKET" --days "$DAYS" --outdir "$WORK/win" \
    "${MINFLAG[@]}" "${RSFLAG[@]}" --jobs "$JOBS" --gamma-threads $((JOBS * 2)) --assets "$ASSETS" \
    --map-s3 "$MAPKEY" > "$LOG/clobwin.log" 2>&1 || { tail -20 "$LOG/clobwin.log"; die "clobwin упал (полный лог: $LOG/clobwin.log)"; }
  # Проверка «каждые сутки на месте», а не «файл существует»: clobwin помечает сутки
  # готовыми по непустому файлу, и без этого шага обрезанный датасет (обрыв стрима на
  # ползаписи — ровно то, что случилось 20.09 с exit 2) вошёл бы во все прогоны как
  # настоящий. Число строк печатается, потому что «1200 против 300» видно только глазами.
  ls "$WORK"/win/windows_*.csv >/dev/null 2>&1 || die "после clobwin нет ни одного windows_*.csv"
  echo "   строки по суткам:"
  for f in "$WORK"/win/windows_*.csv; do
    N=$(grep -c . "$f" 2>/dev/null || echo 0)
    printf '     %s: %s\n' "$(basename "$f" .csv)" "$N"
    [[ "$N" -lt 50 ]] && die "$(basename "$f"): $N строк — сутки обрезаны; rm этого файла и повтори STEPS=clobwin"
  done
  # Итог пассов в общий лог: без этих чисел прогоны неотличимы («датасет пустой из-за
  # трубы» vs «из-за того, что в сутках правда нечего размечать») — а $LOG/clobwin.log
  # никто не присылает, присылают run*.log.
  grep -E "passA\] итог|passC\] строк|minutes\] aggTrade" "$LOG/clobwin.log" | tail -6 | sed 's/^/   /'
  ok clobwin
fi

# ---------------------------------------------------------------- 4. venue
if have venue && st venue; then
  say "платёж площадки по ВСЕМ окнам (ptb/fp), $JOBS потоков"
  python3 "$WORK/markcalc.py" --venue-keys "$WORK/win" --venue-key-codes 5m,15m \
    --venue-workers "$JOBS" --venue-cache "$WORK/venue.jsonl" > "$LOG/venue.log" 2>&1 \
    || { tail -20 "$LOG/venue.log"; die "venue-keys упал"; }
  tail -3 "$LOG/venue.log"
  ok venue
fi
VEN=$WORK/venue.jsonl

# ---------------------------------------------------------------- 5. taker (минуты)
if have taker_min && st taker_min && need_min taker_min; then
  say "ТЕЙКЕР-исследование, разрешение = минута (весь набор, сети не требует)"
  python3 "$WORK/takerprobe.py" --win "$WORK/win" --venue "$VEN" --code 5m \
    --assets "$ASSETS" --split-asset --min-n 20 --inv-usd "$INV_USD" \
    > "$LOG/taker-min.txt" 2>&1 || { tail -20 "$LOG/taker-min.txt"; die "takerprobe (мин) упал"; }
  sed -n '1,40p' "$LOG/taker-min.txt"
  ok taker_min
fi

# ---------------------------------------------------------------- 6. taker (1с)
if have taker_1s && st taker_1s; then
  say "ТЕЙКЕР-исследование на 1-секундном разрешении (докачка 1с-клинов, $RATE req/s)"
  python3 "$WORK/takerprobe.py" --win "$WORK/win" --venue "$VEN" --code 5m \
    --assets "$ASSETS" --split-asset --min-n 20 --inv-usd "$INV_USD" \
    --secs-cache "$WORK/kl1s.jsonl" --workers "$JOBS" --rate "$RATE" \
    --max-windows "$MAXW" > "$LOG/taker-1s.txt" 2>&1 \
    || { tail -20 "$LOG/taker-1s.txt"; die "takerprobe (1с) упал"; }
  grep -E "окон в датасете|ДИАГНОЗ|по объёму|докачано|1с-клины:" "$LOG/taker-1s.txt" | head -8
  sed -n '/Т1. Критерий «twap»/,/Т2\./p' "$LOG/taker-1s.txt"
  if grep -q "разрешение: 1 СЕКУНДА" "$LOG/taker-1s.txt"; then
    ok taker_1s
  else
    # Маркер НЕ ставится: иначе одна сетевая заминка навсегда locking-ает шаг, и
    # «тейкер на секундах» превращается в минутный прогон с гордым названием 1s.
    rm -f "$WORK/.done_taker_1s"
    echo "   ВНИМАНИЕ: 1с-клины не пришли (Binance/451/лимит) — шаг НЕ помечен" \
         "готовым, повторится при следующем запуске. Числа в taker-1s.txt сейчас" \
         "минутные, и вывод «последние секунды неизмеримы» из них делать нельзя."
    [[ "$MINUTES" == "0" ]] && echo "   (MINUTES=0: минутного фолбэка нет вовсе — шаг повторится, когда Binance отвечает)"
  fi
  echo "   если «докачать N» > лимита — повтори шаг 6 с бо́льшим MAXW (кэш дополняется)"
fi

# ---------------------------------------------------------------- 7. mm (гейт)
if have mm_gate && st mm_gate && need_min mm_gate; then
  say "MM-сим, ГЕЙТНАЯ конфигурация (k=2.5, f=0.5¢, прокси-ярлык) — сравнимо с M5"
  python3 "$WORK/clobmm.py" --win "$WORK/win" --codes 5m,15m --k 2.5 --fill-margin 0.5 \
    > "$LOG/mm-gate.txt" 2>&1 || { tail -20 "$LOG/mm-gate.txt"; die "clobmm (гейт) упал"; }
  grep -E "^primary|^контроль|вердикт|ДА |НЕТ |ИТОГ" "$LOG/mm-gate.txt" | head -12
  ok mm_gate
fi

# ---------------------------------------------------------------- 8. mm (ёмкость)
if have mm_capacity && st mm_capacity && need_min mm_capacity; then
  say "MM-сим на ДЕНЬГАХ ПЛОЩАДКИ + заявка \$$INV_USD + фильтр участия ${MAXPART}"
  echo "   это ДРУГОЙ стандарт метки: с гейтом M5 эти числа не сравниваются, они"
  echo "   отвечают на «было ли чем торговать»"
  python3 "$WORK/clobmm.py" --win "$WORK/win" --codes 5m,15m --k 2.5 --fill-margin 0.5 \
    --venue-cache "$VEN" --inventory-usd "$INV_USD" --max-part "$MAXPART" \
    > "$LOG/mm-capacity.txt" 2>&1 || { tail -20 "$LOG/mm-capacity.txt"; die "clobmm (ёмкость) упал"; }
  sed -n '/ЁМКОСТЬ/,/вердикты/p' "$LOG/mm-capacity.txt"
  grep -E "^primary|ярлык = |отброшено" "$LOG/mm-capacity.txt" | head -6
  # Если фильтр участия съел почти всё, «ёмкость = 0» неотличима от «данных нет».
  # Дешевле переснять без фильтра (секунды), чем потом неделю спорить о пустой таблице.
  REJ=$(grep 'отброшено по «тонкому окну»' "$LOG/mm-capacity.txt" \
         | grep -oE '[0-9]+ из [0-9]+' | head -1)
  if [[ -n "$REJ" ]]; then
    R=$(echo "$REJ" | awk '{printf "%.0f", ($3>0)?100*$1/$3:0}')
    if [[ "$R" -gt 50 ]]; then
      echo "   фильтр участия отсёк ${R}% окон — переснимаем ЁМКОСТЬ без него, чтобы"
      echo "   «нулевая ёмкость» не была неотличима от «нет данных»"
      python3 "$WORK/clobmm.py" --win "$WORK/win" --codes 5m,15m --k 2.5 --fill-margin 0.5 \
        --venue-cache "$VEN" --inventory-usd "$INV_USD" --max-part 0 \
        > "$LOG/mm-capacity-open.txt" 2>&1 || echo "   (пересъёмка упала — смотри $LOG/mm-capacity.txt)"
      sed -n '/ЁМКОСТЬ/,/вердикты/p' "$LOG/mm-capacity-open.txt" | head -12
    fi
  fi
  ok mm_capacity
fi

say "ИТОГ"
ls -la "$LOG" | tail -9
echo "прислать (по этим файлам решаем, а не по скриншотам):"
for f in taker-min.txt taker-1s.txt mm-gate.txt mm-capacity.txt; do
  [[ -f "$LOG/$f" ]] && echo "   cat $LOG/$f"
done
echo "размер датасета: $(du -sh "$WORK" 2>/dev/null | cut -f1), из них 1с-кэш $(du -sh "$WORK/kl1s.jsonl" 2>/dev/null | cut -f1)"
echo "перезапустить один шаг: rm -f $WORK/.done_<шаг> && STEPS=<шаг> $0"
