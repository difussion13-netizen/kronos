#!/usr/bin/env bash
# spawn-testbox.sh — тест-бокс M9 (replay трейдера) по образцу запуска калькулятора.
# t3.micro, AL2023 (образ через SSM), профиль kronos-calc (S3-доступ к датасету уже есть),
# default VPC/SG как на проде. Ключ awsirland. Всё в одной REGION.
#   SPOT=1 bash spawn-testbox.sh   — spot-вариант (для реплеев достаточно).
set -euo pipefail
REGION=${REGION:-eu-west-1}
TYPE=${TYPE:-t3.micro}
NAME=${NAME:-kronotest}
PROFILE=${PROFILE:-kronos-calc}
KEY=${KEY:-awsirland}
BUCKET=${BUCKET:-kronolog-moi1234}
DAY=${DAY:-$(date -u -d "2 days ago" +%Y%m%d)}     # целый день: 15.09 последний до аварии

AMI=$(aws ssm get-parameters --region "$REGION" \
  --names /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query 'Parameters[0].Value' --output text)

MK=""
[ "${SPOT:-0}" = "1" ] && MK='--instance-market-options MarketType=spot,SpotOptions={MaxPrice=0.008,SpotInstanceRequestType=one-time}'

IID=$(aws ec2 run-instances --region "$REGION" --image-id "$AMI" --instance-type "$TYPE" \
  --key-name "$KEY" --associate-public-ip-address \
  --iam-instance-profile Name="$PROFILE" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" \
  --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=10,VolumeType=gp3,DeleteOnTermination=true}" \
  $MK --query 'Instances[0].InstanceId' --output text)
aws ec2 wait instance-running --region "$REGION" --instance-ids "$IID"
IP=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$IID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "IID=$IID IP=$IP"

echo "--- бутстрап + прогон реплея (подожди ~40с, пока юнит поднялся) ---"
cat <<EOF
ssh -o StrictHostKeyChecking=accept-new ec2-user@$IP '
  set -e
  mkdir -p /tmp/rt && cd /tmp/rt
  aws s3 cp s3://$BUCKET/kronolog/$DAY/ ./$DAY/ --recursive --only-show-errors --region $REGION
  aws s3 cp s3://$BUCKET/kronos/win/tokens_map.json . --only-show-errors --region $REGION
  curl -fsSLO "https://raw.githubusercontent.com/difussion13-netizen/kronos/arena/01a063d9-kronos/trader/kronotrade.py?nc=m9"
  nohup python3 kronotrade.py --replay . --day $DAY --codes 5m,15m \\
      --journal /tmp/rt/journal.jsonl >/tmp/rt/replay.log 2>&1 &
  sleep 30; tail -2 /tmp/rt/replay.log; wc -l /tmp/rt/journal.jsonl 2>/dev/null
'
# по готовности: tail /tmp/rt/replay.log -> строка "=== replay ... {stats}"; затем
# aws ec2 terminate-instances --region $REGION --instance-ids $IID
EOF
