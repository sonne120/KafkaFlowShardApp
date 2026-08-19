#!/bin/bash
# Runs once on first start of mysql-slave (inside the official image's
# init phase: the local server is up on the unix socket, networking to
# other containers works).
set -euo pipefail

echo "[slave-init] waiting for mysql-master to accept connections..."
until mysql -h mysql-master -uroot -proot -e "SELECT 1" &>/dev/null; do
  sleep 2
done

# Wait until the master's own init scripts have created the repl user —
# probe with the actual replication credentials.
echo "[slave-init] waiting for replication user on master..."
until mysql -h mysql-master -urepl -preplpass -e "SELECT 1" &>/dev/null; do
  sleep 2
done

echo "[slave-init] configuring GTID auto-position replication..."
mysql -uroot -proot <<'SQL'
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST            = 'mysql-master',
  SOURCE_PORT            = 3306,
  SOURCE_USER            = 'repl',
  SOURCE_PASSWORD        = 'replpass',
  SOURCE_AUTO_POSITION   = 1,
  GET_SOURCE_PUBLIC_KEY  = 1;
START REPLICA;
SQL

echo "[slave-init] replica configured. GTID auto-position will survive the"
echo "[slave-init] entrypoint's restart of mysqld — replication resumes on boot."
