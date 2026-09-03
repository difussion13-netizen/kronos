# Инструкция: выбрать сервер на AWS и запустить логгер (для новичка)

Обновлено: 2026-09-03. Код логгера лежит в этом репозитории, папка `logger/`,
ветка `arena/01a063d9-kronos` (репозиторий публичный, скачивается без пароля).

Что мы делаем в целом: арендуем в «облаке» Amazon (AWS) маленький компьютер, который
никогда не выключается, ставим на него программу-логгер, и он 24/7 записывает «показания
часов» рынков (Polymarket и Binance) в ваше личное облачное хранилище. Логи — это будущая
база для обучения и проверки бота; без них историю не восстановить.

Стоимость: примерно **$22 в месяц** ($0 в первый месяц, если аккаунт новый — там есть
бесплатный лимит). Всё, что делаем — вкладки в браузере и вставки готовых команд.

---

## Шаг 1. Зарегистрироваться в AWS и выбрать «регион»

1. Зайдите на https://aws.amazon.com — кнопка **Create an Account**. Понадобятся карта
   (списаний не будет, только проверка), телефон, адрес. Подтвердите аккаунт по SMS/звонку.
   Если аккаунт уже есть — просто войдите.
2. В правом верхнем углу страницы — список «регион» (зона дата-центров; от неё
   зависят цены и задержки). Выберите **Europe (Ireland) — eu-west-1**.
   (Поправка 2026-09-03: раньше тут значилась Вирджиния.) По официальной документации
   Polymarket их торговые серверы стоят в **Лондоне (AWS eu-west-2)**; США и
   Великобритания — в списке «нельзя открывать новые ордера» (close-only). Ближайший
   незапрещённый регион — **eu-west-1 (Дублин)**, ~1–3 мс до Лондона. Источник:
   docs.polymarket.com, страница «Geographic Restrictions», раздел Server Infrastructure.
   Для бота важна не география как таковая, а то, как IP «выглядит» в глазах Polymarket —
   проверяется одной командой (Шаг 8, geoblock).
3. Защита от неожиданностей: в меню сверху найдите **Billing → Budgets → Create budget**:
   тип **Monthly cost budget**, сумма **$25**, алерт на 80% ($20) на вашу почту.
   Теперь, если что-то начнёт тратить больше, Amazon пришлёт письмо.

## Шаг 2. Открыть «облачный терминал» (ничего устанавливать не надо)

В AWS есть встроенное окошко для команд — **CloudShell**. Вверху справа нажмите иконку
`>_` («CloudShell»). Откроется чёрное окно в браузере, уже знакомое с вашим аккаунтом.
Все команды ниже вставляются туда (правой кнопкой мыши → вставить) и выполняются Enter'ом.

Первая команда — «указываем регион» (выполнить один раз в этом окне):

```bash
export AWS_REGION=eu-west-1 AWS_DEFAULT_REGION=eu-west-1
```

## Шаг 3. Создать хранилище для логов (S3)

S3 — это «безлимитный сетевой диск» Amazon, с него данные не теряются даже когда сервер
выключен. Имя бакета (так называется «папка верхнего уровня») должно быть уникальным для
всего мира — придумайте свой суффикс из цифр.

Вставьте блок целиком (поменяйте 1234 на любые 4–6 цифр/букв — имя бакета уникально
на весь мир; вписывайте их **просто текстом**, без скобок `$( )` — в bash скобки со
знаком доллара означают «выполни программу», а не «подставь сюда»):

```bash
BUCKET=kronolog-1234
# вне Вирджинии AWS требует явно указать регион бакета, иначе ошибка
# IllegalLocationConstraintException:
aws s3api create-bucket --bucket $BUCKET \
  --create-bucket-configuration LocationConstraint=eu-west-1
aws s3api put-public-access-block --bucket $BUCKET \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
# проверка, что бакет встал куда надо (ждём "LocationConstraint": "eu-west-1"):
aws s3api get-bucket-location --bucket $BUCKET
```

«Что должно получиться»: первая команда — тишина или `{}` (это успех, «создал»),
вторая — тишина (это успех), третья — `"LocationConstraint": "eu-west-1"`.
Второй блок — «запрет публичного доступа», чтобы ваши логи не увидел никто из
интернета.

## Шаг 4. Выдать серверу «пропуск» вместо паролей

Мы не будем хранить на сервере никакие ключи-пароли от хранилища. Вместо этого создаём
«роль» (пропуск) и вешаем её на сервер — Amazon сам всё проверит. Три команды одной
вставкой:

```bash
aws iam create-role --role-name kronolog --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam attach-role-policy --role-name kronolog --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam put-role-policy --role-name kronolog --policy-name s3-write --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:AbortMultipartUpload\",\"s3:ListBucket\"],\"Resource\":[\"arn:aws:s3:::$BUCKET\",\"arn:aws:s3:::$BUCKET/*\"]}]}"
aws iam create-instance-profile --instance-profile-name kronolog
aws iam add-role-to-instance-profile --instance-profile-name kronolog --role-name kronolog
```

(Первая — создать роль; вторая — разрешить Amazon'у подключаться к серверу для удалённой
работы без SSH-ключей; третья — разрешить серверу писать в ваш бакет; последние две —
«профиль пропуска», который навесим на сервер.)

## Шаг 5. Запустить сервер

Сервер в AWS называется «экземпляр» (instance). Берём самый дешёвый подходящий —
`small` с двумя ядрами. Входящих портов не открываем вообще (доступ к терминалу —
через тот же SSM, безопаснее), поэтому создаём пустую «сетевую группу безопасности»:

```bash
# важно: свежий AWS CLI не принимает старые короткие флаги (-g-name, -d) —
# только полные имена с двумя минусами, как ниже
SGID=$(aws ec2 create-security-group --group-name kronolog --description "no ingress" \
  --query GroupId --output text)
aws ec2 enable-ebs-encryption-by-default
aws ec2 run-instances \
  --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --instance-type t3.small \
  --security-group-ids $SGID \
  --iam-instance-profile Name=kronolog \
  --block-device-mappings DeviceName=/dev/xvda,Ebs={VolumeSize=16,VolumeType=gp3} \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=kronolog}]' \
  --query InstanceId --output text
```

Последняя строчка выведет идентификатор вида `i-0abc123...` — **запишите его**, он понадобится.
Ждём ~40 секунд сервер загружается. (Образ ОС — «Amazon Linux 2023», свежая версия
подтянется автоматически, адрес образа — это «самый свежий, обнови».)

## Шаг 6. Зайти на сервер (без паролей и ключей)

1. В AWS меню → **EC2** → слева **Instances**. Отметьте галочкой `kronolog`.
2. Сверху кнопка **Connect** → вкладка **Session Manager** → **Connect**.
   Откроется ещё одно чёрное окошко — это и есть терминал сервера. Если пишет
   «still booting» — подождите полминуты и повторите.

## Шаг 7. Установить логгер

Вставляйте в окошко сервера блоки по очереди:

```bash
# 7.1 — скачать наш код из GitHub (ветка указана; паролей не просит)
sudo dnf install -y git rsync python3-pip
sudo git clone -b arena/01a063d9-kronos https://github.com/difussion13-netizen/kronos.git /tmp/kronos

# 7.2 — установщик: создаёт пользователя-службу, ставит программу и автозапуск
cd /tmp/kronos/logger && sudo bash install.sh

# 7.3 — подсказываем программе, куда грузить логи (имя бакета с шага 3!)
sudo sed -i 's|bucket: ""|bucket: "kronolog-1234"|' /opt/kronolog/app/config.yaml
sudo systemctl restart kronolog
```

(В 7.3 замените `kronolog-1234` на свой бакет. Если репозиторий когда-нибудь станет
приватным — скачайте папку `logger` архивом с GitHub (кнопка Code → Download ZIP) и
закиньте на сервер через «Session Manager → Actions → Upload file», остальное то же самое.)

## Шаг 8. Проверить, что всё работает

```bash
# где сервер «на самом деле» по версии AWS (должно быть "region": "eu-west-1"):
curl -s http://169.254.169.254/latest/dynamic/instance-identity/document

# как этот сервер «видит» Polymarket — ждём: "blocked": false и "country": "IE":
curl -s https://polymarket.com/api/geoblock

# что программа жива (должно быть active (running)):
systemctl status kronolog | head -5

# «пульс» потоков: у rtds и binance age_last_msg_s должен быть меньше 60 секунд,
# u clob обычно тоже маленький (окна шевелятся постоянно)
cat /var/lib/kronolog/status.json

# последние строки журнала ошибок:
journalctl -u kronolog -n 20 --no-pager

# через 20–30 минут — проверить, что файлы доехали до хранилища (в CloudShell, не на сервере):
aws s3 ls s3://kronolog-1234/kronolog/rtds/$(date -u +%Y%m%d)/
```

Нормальная картина: `status.json` обновляется каждые 15 секунд, ошибок нет, а вечером в
бакете лежат файлы `*.jsonl.gz`. Обе первые проверки — про будущее: логгеру гео-статусы
не мешают вообще (он только читает), но если когда-то сюда переедет торговый бот,
`geoblock: blocked=false, country=IE` — его пропуск. Если country не IE — не чиним
«пингами», это свойство IP-пула: перезапуск инстанса часто меняет адрес, либо берём
Elastic IP в этом же регионе и проверяем заново (подробности: `research-server-location.md`). Дальше программа работает сама: перезапустится после
сбоев, догонит обрывы связи, переживёт перезагрузку сервера (автозапуск настроен).

## Бытовая гигиена (что и когда делать)

- **Зачем вообще сервер**: он обязан гореть 24/7; ваш домашний компьютер не подходит —
  отключится ночью и потеряет часы невосстановимых данных (Chainlink/стаканы Polymarket).
- **Раз в неделю** (5 секунд): открыть `systemctl status kronolog` в Session Manager —
  `active (running)`? Или раз в месяц зайти в S3 и глянуть размер папки (должно расти
  ~0.4–0.7 GB/день).
- **Перезагрузка/остановка сервера**: данные в S3 не теряются никогда; локальный буфер
  (до 3 дней) переживёт остановку, если не удалять сам сервер (Stop vs Terminate).
  Никогда не жмите **Terminate** — это полное удаление машины (данные в S3 останутся).
- **Если захотите выключить всё**: Stop instance — платиться будет только $1.3 за диск и
  копейки за S3. Terminate instance + удалить роль/группу — тогда совсем ноль, кроме S3.
- **Никому не давайте** доступ к аккаунту AWS: в аккаунте живут и бакет, и биллинг.

## Если что-то не так

| Симптом | Причина и лечение |
|---|---|
| `git clone` просит пароль | репозиторий стал приватным → сделать Public (GitHub: Settings → Danger zone) или скачать ZIP и залить Upload file |
| Session Manager не подключается | сервер ещё грузится (подождать), либо пропустили шаг 4 вторую команду (право SSM) |
| upload fail в логах | в `config.yaml` имя бакета не то, или роль не прицепилась: `aws ec2 describe-instances --filters Name=instance-state-name,Values=running --query 'Reservations[].Instances[].[Tags[0].Value,IamInstanceProfile.Value]'` |
| `age_last_msg_s` у потока большой и растёт | связь с биржей упала (или биржа чинится) — программа сама переподключается; проверьте `journalctl`, и если час не помогло — перезапуск: `sudo systemctl restart kronolog` |
| instance-профиль «in use» при повторном запуске шага 4 | вы уже создавали его ранее — пропустите две последние команды шага 4 |
