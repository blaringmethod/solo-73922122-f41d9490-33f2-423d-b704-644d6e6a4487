#!/usr/bin/env bash
# Demonstration against the real docker-compose stack (db + two API instances
# behind nginx on :8080).
#
#   bash scripts/demo_compose.sh
#
# Shows:
#   1. strict intake + idempotent retry / conflicting-content rejection;
#   2. the drift rule (first three released, then a >5nm drift is quarantined,
#      quarantined batches never enter the reference window);
#   3. slow head-of-line batch: same-device FIFO, other devices unaffected;
#   4. container crash: `docker stop`-like failure mid-batch, survivor takes
#      over with no half batch and no double reference advancement;
#   5. the data endpoint serves released batches only.
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"
DEV="EQ-DEMO-$$"
j() { python3 -c "import sys,json;d=json.load(sys.stdin);print(json.dumps(d,ensure_ascii=False,indent=2))"; }

csv() { # $1=prefix $2=thickness $3=count
  { echo "wafer_no,thickness_nm"
    for i in $(seq 1 "$3"); do printf "%s-W%03d,%s\n" "$1" "$i" "$2"; done; }
}

post() { # $1=batch $2=device $3=file
  curl -sS -w '\n%{http_code}\n' -X POST "$BASE/batches" \
    -F "batch_no=$1" -F "device_id=$2" -F "file=@$3;type=text/csv"
}
status_of() { curl -sS "$BASE/batches/$1" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])'; }
wait_decided() { # $1=batch
  for _ in $(seq 1 100); do
    s=$(status_of "$1")
    [ "$s" != "PENDING" ] && { echo "$s"; return; }
    sleep 0.3
  done
  echo "TIMEOUT"; exit 1
}

echo "== 0. health =="
curl -sS "$BASE/health" | j

echo; echo "== 1. intake: first three baselines on $DEV =="
for i in 1 2 3; do
  csv "D$i" 500 5 > "/tmp/demo_d$i.csv"
  post "DEMO-B$i" "$DEV" "/tmp/demo_d$i.csv" | tail -1
done
for i in 1 2 3; do echo "DEMO-B$i -> $(wait_decided DEMO-B$i)"; done

echo; echo "== 2. drift: +6nm quarantined, exactly +5nm released =="
csv DQ 506 5 > /tmp/demo_q.csv
post DEMO-Q "$DEV" /tmp/demo_q.csv | tail -1
echo "DEMO-Q -> $(wait_decided DEMO-Q)"
csv DE 505 5 > /tmp/demo_e.csv
post DEMO-E "$DEV" /tmp/demo_e.csv | tail -1
echo "DEMO-E -> $(wait_decided DEMO-E)"
curl -sS "$BASE/batches/DEMO-Q" | j

echo; echo "== 3. idempotent retry reused; different content rejected (409) =="
post DEMO-B1 "$DEV" /tmp/demo_d1.csv | j | grep -E 'reused_retry|status'
csv DIFF 501 5 > /tmp/demo_diff.csv
echo "conflict http status: $(post DEMO-B1 "$DEV" /tmp/demo_diff.csv | tail -1)"

echo; echo "== 4. slow head-of-line batch on $DEV-SLOW (other device flows) =="
SLOWDEV="${DEV}-SLOW"; OTHERDEV="${DEV}-OTHER"
csv S1 500 5 > /tmp/s1.csv; csv S2 500 5 > /tmp/s2.csv
csv O1 700 5 > /tmp/o1.csv; csv O2 700 5 > /tmp/o2.csv
# Mark SLOW-1 slow by batch number BEFORE uploading it, so the marker is in
# place before any worker can pick the batch up (deterministic).
docker compose exec -T api1 sh -c "echo 'SLOW-1:8' > /data/raw/slow_batches.txt"
post SLOW-1 "$SLOWDEV" /tmp/s1.csv >/dev/null
# Uploads stay available while SLOW-1 is being processed.
post SLOW-2 "$SLOWDEV" /tmp/s2.csv >/dev/null
post OTHER-1 "$OTHERDEV" /tmp/o1.csv >/dev/null
post OTHER-2 "$OTHERDEV" /tmp/o2.csv >/dev/null
sleep 2
echo "  while SLOW-1 pending: SLOW-1=$(status_of SLOW-1), OTHER-2=$(status_of OTHER-2)"
[ "$(status_of SLOW-1)" = "PENDING" ] && [ "$(status_of OTHER-2)" = "RELEASED" ] \
  || { echo "FAIL: slow head batch blocked the other device"; exit 1; }
echo "  SLOW-1 -> $(wait_decided SLOW-1), SLOW-2 -> $(wait_decided SLOW-2)"
docker compose exec -T api1 sh -c 'rm -f /data/raw/slow_batches.txt'

echo; echo "== 5. container crash mid-batch and survivor takeover =="
CDEV="${DEV}-CRASH"
# Ingest four batches with NO worker running (both APIs stopped, a workerless
# one-shot harness submits), so the crash is guaranteed to hit a PENDING
# batch.  The crash marker is written by the same harness, keyed by the
# batch number CRASH-1.
docker compose stop api1 api2
docker compose run --rm -T -e INSTANCE_ID=ingest api1 \
  python -m scripts.worker_harness --no-worker --crash CRASH-1 \
  --submit "CRASH-1:$CDEV:500:5,CRASH-2:$CDEV:500:5,CRASH-3:$CDEV:500:5,CRASH-4:$CDEV:504:5"

# A dedicated one-shot worker hard-exits (os._exit, like kill -9) while
# deciding CRASH-1; the APIs stay down so nothing else interferes.
docker compose run --rm -T -e INSTANCE_ID=crasher api1 \
  python -m scripts.worker_harness --exit-when-idle --idle-timeout 30 || true
echo "  rows after crash (CRASH-1 must stay PENDING, no median):"
docker compose exec -T db psql -U "${POSTGRES_USER:-fab}" -d "${POSTGRES_DB:-fab}" -c \
  "SELECT batch_no, status, median_nm, attempts FROM batches WHERE device_id='$CDEV' ORDER BY accept_seq"

# Remove the crash marker BEFORE the APIs come back, then let the surviving
# instances drain the queue.
docker compose run --rm -T api1 sh -c 'rm -f /data/raw/crash_batches.txt'
docker compose start api1 api2
for b in CRASH-1 CRASH-2 CRASH-3 CRASH-4; do echo "  $b -> $(wait_decided "$b")"; done
echo "  final decision + references for the post-baseline batch CRASH-4:"
curl -sS "$BASE/batches/CRASH-4" | j

echo; echo "== 6. data lake endpoint hides quarantined batches =="
curl -sS "$BASE/data/released?device_id=$DEV" | j | grep -E 'total_released|batch_no'
echo
echo "OpenAPI docs: $BASE/docs"
