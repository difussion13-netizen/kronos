#!/usr/bin/env bash
# run-trajectory.sh — исследование Polymarket → Binance edge на новом калькуляторе.
# Sync минуток/окон из S3 + selftest + прогон R1/R2/R3.
#   bash run-trajectory.sh           # всё
#   bash run-trajectory.sh --selftest # только проверка
set -euo pipefail

WORK=${WORK:-/tmp/calc}
BUCKET=kronolog-moi1234
REPO="https://raw.githubusercontent.com/difussion13-netizen/kronos/arena/01a063d9-kronos"
mkdir -p "$WORK" "$WORK/win" "$WORK/logs"

# 1. Sync минуток и окон из S3
echo "=== sync из s3://$BUCKET/kronos/win/ ==="
aws s3 cp "s3://$BUCKET/kronos/win/" "$WORK/win/" --recursive \
    --exclude '*' --include 'windows_*.csv' --include 'minutes_*.csv' \
    --include 'tokens_map.json' --only-show-errors
echo "минутки: $(ls "$WORK"/win/minutes_*.csv 2>/dev/null | wc -l) суток"
echo "окна:    $(ls "$WORK"/win/windows_*.csv 2>/dev/null | wc -l) суток"

# 2. Скачать trajectory.py
echo "=== trajectory.py ==="
curl -fsSL -o "$WORK/trajectory.py" "$REPO/analysis/trajectory.py?nc=$(date +%s)"
python3 -m py_compile "$WORK/trajectory.py"
echo "скачан и компилируется"

# 3. Selftest
echo "=== selftest ==="
python3 "$WORK/trajectory.py" --selftest

# 4. Полный прогон (если не --selftest)
if [[ "${1:-}" != "--selftest" ]]; then
  echo "=== прогон R1/R2/R3 ==="
  python3 "$WORK/trajectory.py" --win "$WORK/win" --outdir "$WORK/traj" 2>&1 \
    | tee "$WORK/logs/trajectory.log"
  echo "результат: cat $WORK/logs/trajectory.log"
fi