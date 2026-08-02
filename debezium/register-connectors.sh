#!/usr/bin/env sh
# Registers one Debezium MongoDB connector per shard against Kafka Connect.
# POSIX sh (the curlimages/curl image has no bash) — no arrays, no bashisms.
#
# Each mongod runs as a single-node replica set (rs0) — change streams require a replica set —
# and is captured into its own topic prefix. The SMT chain:
#   ExtractNewDocumentState -> flatten the Mongo doc, add __op
#
# NOTE: keying by client_id is intentionally left off. The read model's aggregates are
# commutative (per-protocol counts), so partition ordering is irrelevant, and the Postgres
# version-guard already orders any last-value view. Forcing a client_id key via ValueToKey
# would hard-fail the task on any document missing the field — robustness wins here.
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"

# space-separated "shard:host" pairs (each host is its own rs0 replica set)
SHARDS="https:mongo-https tcp:mongo-tcp udp:mongo-udp arp:mongo-arp other:mongo-other"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
  sleep 3
done
echo "Connect is up."

for entry in $SHARDS; do
  name="${entry%%:*}"
  host="${entry##*:}"
  connector="mongo-${name}"

  echo "Registering ${connector} (host=${host}) ..."
  curl -fsS -X PUT \
    -H "Content-Type: application/json" \
    "${CONNECT_URL}/connectors/${connector}/config" \
    -d @- <<JSON
{
  "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
  "tasks.max": "1",
  "mongodb.connection.string": "mongodb://${host}:27017/?replicaSet=rs0",
  "topic.prefix": "pcap.${name}",
  "capture.mode": "change_streams_update_full",
  "database.include.list": "pcap",
  "collection.include.list": "pcap.packets",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
  "transforms.unwrap.add.fields": "op"
}
JSON
  echo
done

echo "All connectors registered."
