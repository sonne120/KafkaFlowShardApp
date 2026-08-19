#!/bin/bash
# PostMasterFailoverProcesses hook.
# Belt-and-suspenders fencing: the revived ex-master already boots
# super_read_only (replication.cnf) and ProxySQL's check_type keeps it out
# of the writer hostgroup. This hook additionally severs EXISTING
# connections to the dead/failed node (OFFLINE_HARD kills live conns) —
# closes the window where an in-flight srv_pub transaction opened before
# the crash could still be attached to the old node.
#
# $1 = {failedHost} from orchestrator
set -euo pipefail

FAILED_HOST="${1:?failedHost argument required}"

echo "[fence] hard-offlining ${FAILED_HOST} in ProxySQL"

mysql -h proxysql -P 6032 -u admin -padmin --ssl-mode=DISABLED -e "
  UPDATE mysql_servers SET status='OFFLINE_HARD' WHERE hostname='${FAILED_HOST}';
  LOAD MYSQL SERVERS TO RUNTIME;
  SAVE MYSQL SERVERS TO DISK;"

echo "[fence] done. To re-admit the node after it has been re-parented as a"
echo "[fence] replica (orchestrator does the re-parenting), run:"
echo "[fence]   UPDATE mysql_servers SET status='ONLINE' WHERE hostname='${FAILED_HOST}';"
echo "[fence]   LOAD MYSQL SERVERS TO RUNTIME; SAVE MYSQL SERVERS TO DISK;"
