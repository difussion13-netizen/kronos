# kronolog — логгер данных для исследования Polymarket BTC 5m/15m/4h

Пишет в сжатые JSONL-чанки и выгружает в S3 три сырых потока без парсинга
(raw verbatim, с приёмным наносекундным таймстампом):

| Поток | Что это | Зачем |
|---|---|---|
| `rtds` | Polymarket RTDS WS: Chainlink BTC/USD(+ETH/SOL/XRP) и Binance-цены | **источник резолва** — единственный невоспроизводимый ряд; метка для бэктеста; лаг Binance→Chainlink |
| `clob` | CLOB market WS: книги/прайс-чейнджи/трейды активных up/down-окон (авто-обнаружение окон через Gamma, детерминированные слаги `btc-updown-5m-<epoch>`) | net-spread, очереди, токсичность филов — экономика MM |
| `binance` | combined WS `@aggTrade @kline_1m @depth5@100ms` | живая микроструктура (depth — единственное, что не докачаешь из vision-архивов) |

Сырые данные сознательно не парсятся: протоколы меняются, парсеры живут в
аналитическом слое; здесь только приём, обёртка `{"t": ns, "raw": ...}`, gzip(-1),
ротация 15 мин, выгрузка в S3 с ретраями, идемпотентность (имя файла = старт части).

## Деплой на AWS (минимальная конфигурация)

**Регион: `eu-west-1` (Ирландия).** (Поправка 2026-09-03: раньше тут рекомендовался
us-east-1 по неверному предположению, будто origin Polymarket в Вирджинии; по официальной
доке он в **eu-west-2 (Лондон)**, а США/UK — close-only для открытия ордеров; ближайший
разрешённый регион — eu-west-1, ~1–3 мс до origin. См. `docs/research-server-location.md`.)
Для *записи* логов регион безразличен (публичное чтение не блокируется), но всё — EC2,
S3, будущий GPU и торговый бокс — держим в одном регионе: intra-region трафик бесплатный,
а торговый контур из US/EU-запрещённых зон открыть ордера не сможет.

### Ресурсы (всё, что нужно)

```bash
export AWS_REGION=eu-west-1
BUCKET=kronolog-data-7f3k2p                      # имя своё (уникальное, строчные;
#     НЕ оборачивайте в $( ) — в bash это «выполни программу», а не «впиши сюда»)

# 1) бакет + life-cycle (gratuitно, Standard первые 90 дней — не трогаем)
# Outside Virginia an explicit location is required, otherwise you'll get
# IllegalLocationConstraintException:
aws s3api create-bucket --bucket $BUCKET \
  --create-bucket-configuration LocationConstraint=eu-west-1
aws s3api put-public-access-block --bucket $BUCKET \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

# 2) роль для инстанса (ключей на диске нет!)
aws iam create-role --role-name kronolog --assume-role-policy-document '{
 "Version":"2012-10-17","Statement":[{"Effect":"Allow",
 "Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam put-role-policy --role-name kronolog --policy-name s3-write --policy-document "{
 \"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",
 \"Action\":[\"s3:PutObject\",\"s3:AbortMultipartUpload\"],
 \"Resource\":\"arn:aws:s3:::$BUCKET/*\"},
 {\"Effect\":\"Allow\",\"Action\":[\"s3:ListBucket\"],
 \"Resource\":\"arn:aws:s3:::$BUCKET\"}]}"
aws iam create-instance-profile --instance-profile-name kronolog
aws iam add-role-to-instance-profile --instance-profile-name kronolog \
  --role-name kronolog

# 3) SG без входящих портов (доступ через SSM)
aws ec2 create-security-group -g-name kronolog --description none

# 4) инстанс: t3.small on-demand, 16GB gp3 (Amazon Linux 2023 — SSM-параметр всегда актуален)
aws ec2 run-instances \
  --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --instance-type t3.small --security-group-ids sg-XXXXXXXX \
  --iam-instance-profile Name=kronolog \
  --block-device-mappings DeviceName=/dev/xvda,Ebs={VolumeSize=16,VolumeType=gp3} \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=kronolog}]'

# (Ubuntu 24.04 тоже подходит — install.sh сам определяет apt/dnf;
#  id SG возьмите из вывода `aws ec2 create-security-group`)
```

**Конфигурация и почему именно она:**

| Ресурс | Выбор | $/мес | Почему |
|---|---|---|---|
| EC2 | `t3.small` on-demand (2 vCPU, 2 GB) | ~$15.2 | запас по CPU 3–5× на пиковые секунды (gzip level 1); spot t3.small ~$5–8 экономит ~$7, но рвёт запись раз в неделю-две; для «бесплатных 12 мес» можно `t3.micro` — на пике BTC-новостей упрётся в CPU credits, рискуем пропусками depth. Брать small on-demand. |
| EBS | 16 GB gp3 на root | ~$1.3 | 3 суток буфера при 0.4–0.7 GB/сутки gz; отдельный том не нужен |
| S3 | тот же регион, Standard | ~$1–2 | 12–21 GB/мес gz |
| Public IPv4 | авто-assign (не EIP) | $3.65 | исходящий трафик для WS; EIP не нужен (реинстанс = новый id, а вход через SSM) |
| CloudWatch | только базовые метрики | 0 | selftest-алерты пишем позже |
| **Итого** | | **≈ $21–22/мес** | |

Никаких NAT (443 наружу через IGW достаточно), ALB, RDS, EKS.
Поток наружу — только WS-запросы; egress трафик ~50–150 MB/сутки (≈$0.05).

### Установка софта

```bash
# с ноутбука, после `aws ssm start-session`:
sudo apt-get install -y git
git clone <ваш-репо> /tmp/kronos && cd /tmp/kronos/logger
sudo bash install.sh
# вписать бакет:
sudo sed -i 's/bucket: ""/bucket: '$BUCKET'/' /opt/kronolog/app/config.yaml
sudo systemctl restart kronolog
```

`install.sh` ставит venv, unit, создаёт юзера `kronolog`, каталог данных
`/var/lib/kronolog`. Файлы: `kronolog.py`, `config.yaml`, `kronolog.service`,
`install.sh`, `requirements.txt`.

### Проверка (первые 10 минут)

```bash
systemctl status kronolog            # active; в логах "kronolog up: streams=[rtds,binance,clob]"
journalctl -u kronolog -f            # reconnect-предупреждения исчезли через минуту
cat /var/lib/kronolog/status.json    # у каждого потока age_last_msg_s < 60 (clob может молчать — окно тихое)
ls /var/lib/kronolog/*/              # чанки jsonl.gz каждые 15 мин
aws s3 ls s3://$BUCKET/kronolog/rtds/$(date -u +%Y%m%d)/   # выгрузка идёт
# быстрый smoke-режим (N секунд и выход):
sudo -u kronolog /opt/kronolog/venv/bin/python /opt/kronolog/app/kronolog.py \
  --config /opt/kronolog/app/config.yaml --once 60
# офлайн-тест пайплайна записи/ротации без сети:
python3 kronolog.py --selftest 5
```

### Контроль полноты (раз в сутки — «молчание» потока = дыра в датасете)

```bash
zstd/gzip-счётчик по часам для каждого стрима:
  for f in /var/lib/kronolog/binance/*.jsonl.gz; do zcat $f | wc -l; done
```
Нормы: binance ≥ 1–5 млн строк/сутки; rtds ≥ 200k; clob зависит от активности окон
(в тихие ночи может <50k). После 2 недель записываемый объём ≈ 8–15 GB gz/нед на BTC
(с 4 активами в clob — ×1.5–2).

## Известные тонкости

- RTDS-подписка: `filters` — **строка** с JSON (в Rust-SDK Polymarket это баг #136);
  heartbeat — текст `"PING"` каждые 5 с (сервер отвечает `"PONG"`, он глушится).
- CLOB market WS не поддерживает надёжный unsubscribe: при закрытии окна шлём чистый
  reconnect (потеря ~1–3 с в момент границы окна — все окна перекрываются lookahead'ом,
  это не дыра; `clob_meta` логирует переподписки).
- Тик-схема CLOB меняется — подписочный шаблон вынесен в `config.yaml`.
- Binance `@depth5@100ms` — partial book, без начального снапшота (для наших фич
  достаточно; полный L2 в следующих фазах при необходимости — через REST-снапшоты).
- Слоты окон: epoch-выровненные `now//S*S` (UTC), как у Polymarket. Если слаг серии
  сменится (история уже меняла `btc-updown-15m-…`) — discovery просто перестанет
  находить окна; алерт по `clob_meta` пустым окнам — TODO при смене протокола.
- Все размеры/имена файлов = UTC; приёмный timestamp — `time.time_ns()` до парсинга.

## Стоимость владения этим датасетом

~0.5 GB gz/сутки (BTC-конфиг: rtds+clob[btc,eth,sol,xrp]+binance-lite) →
S3 ≈ 15 GB/мес ≈ $0.35 + запросы ≈ 0. Итог: весь контур «накопление» ≈ **$22/мес**.
