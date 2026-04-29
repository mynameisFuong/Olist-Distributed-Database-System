#!/bin/bash
echo "[metastore] Cho 60s PostgreSQL khoi dong..."
sleep 60

RETRY=0
until (echo > /dev/tcp/hive-metastore-postgresql/5432) 2>/dev/null; do
    RETRY=$((RETRY+1))
    [ $RETRY -ge 24 ] && echo "TIMEOUT" && exit 1
    echo "[metastore] Cho them 5s... ($RETRY/24)"
    sleep 5
done
echo "[metastore] PostgreSQL OK. Cho them 15s..."
sleep 15

echo "[metastore] Chay schematool initSchema..."
/opt/hive/bin/schematool -dbType postgres -initSchema 2>&1
[ $? -ne 0 ] && /opt/hive/bin/schematool -dbType postgres -upgradeSchema 2>&1 || true

echo "[metastore] Khoi dong Hive Metastore..."
/opt/hive/bin/hive --service metastore