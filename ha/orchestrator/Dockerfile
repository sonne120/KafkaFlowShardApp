# Orchestrator + a mysql client so the fencing hook can talk to the
# ProxySQL admin interface (which speaks the MySQL protocol on :6032).
FROM openarkcode/orchestrator:latest

USER root
# Base image lineage varies between releases — try apt (debian) then apk (alpine).
RUN (apt-get update && apt-get install -y --no-install-recommends default-mysql-client && rm -rf /var/lib/apt/lists/*) \
 || (apk add --no-cache mysql-client) \
 || (microdnf install -y mysql && microdnf clean all)

# conf is mounted by compose to /etc/orchestrator.conf.json
CMD ["/usr/local/orchestrator/orchestrator", "--config=/etc/orchestrator.conf.json", "http"]
