#!/bin/bash
# One-shot bootstrap for a CLEAN stack. Appoints mysql-master as the initial
# writer and registers the topology with Orchestrator.
#
# Why SET GLOBAL and not SET PERSIST: on any restart the node must come
# back read-only (replication.cnf). Writability is a runtime appointment,
# never a persisted property — that is the whole split-brain failsafe.
set -euo pipefail

echo "[bootstrap] waiting for mysql-master..."
until mysql -h mysql-master -uroot -proot --skip-ssl -e "SELECT 1" &>/dev/null; do sleep 2; done

echo "[bootstrap] waiting for mysql-slave replication to be configured..."
until mysql -h mysql-slave -uroot -proot --skip-ssl \
      -e "SHOW REPLICA STATUS\G" 2>/dev/null | grep -q "Replica_IO_Running: Yes"; do
  sleep 2
done

echo "[bootstrap] appointing mysql-master as writer (runtime only)..."
mysql -h mysql-master -uroot -proot --skip-ssl -e \
  "SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF;"

echo "[bootstrap] verifying semi-sync is active on the source..."
mysql -h mysql-master -uroot -proot --skip-ssl -e \
  "SHOW STATUS LIKE 'Rpl_semi_sync_source_status';"

echo "[bootstrap] registering topology with orchestrator..."
# discovery of one node walks the whole topology
if command -v curl &>/dev/null; then
  curl -s "http://orchestrator:3000/api/discover/mysql-master/3306" || true
else
  echo "[bootstrap] curl not found in image; discover manually:"
  echo "  curl http://localhost:3000/api/discover/mysql-master/3306"
fi

echo
echo "[bootstrap] done. Writer = mysql-master. Orchestrator UI: http://localhost:3000"
