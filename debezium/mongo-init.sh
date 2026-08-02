#!/usr/bin/env bash
# Promotes each standalone mongod to a single-node replica set (rs0) so Debezium can read its
# change stream. Idempotent: rs.initiate() on an already-initiated node is ignored.
set -euo pipefail

HOSTS=(mongo-https mongo-tcp mongo-udp mongo-arp mongo-other)

for host in "${HOSTS[@]}"; do
  echo "Waiting for ${host} ..."
  until mongosh --quiet --host "${host}" --eval 'db.runCommand({ ping: 1 })' >/dev/null 2>&1; do
    sleep 2
  done

  echo "Initiating replica set on ${host} ..."
  mongosh --quiet --host "${host}" --eval "
    try {
      rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: '${host}:27017' }] });
      print('initiated rs0 on ${host}');
    } catch (e) {
      print('rs0 already initiated on ${host}: ' + e.message);
    }
  "
done

echo "All shards are replica sets."
