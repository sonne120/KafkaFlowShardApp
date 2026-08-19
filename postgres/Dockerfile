FROM postgres:16

ARG PG_IVM_VERSION=1.9

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        postgresql-server-dev-16; \
    curl -fsSL "https://github.com/sraoss/pg_ivm/archive/refs/tags/v${PG_IVM_VERSION}.tar.gz" -o /tmp/pg_ivm.tar.gz; \
    mkdir -p /tmp/pg_ivm; \
    tar -xzf /tmp/pg_ivm.tar.gz -C /tmp/pg_ivm --strip-components=1; \
    make -C /tmp/pg_ivm; \
    make -C /tmp/pg_ivm install; \
    apt-get purge -y --auto-remove build-essential postgresql-server-dev-16 curl; \
    rm -rf /tmp/pg_ivm /tmp/pg_ivm.tar.gz /var/lib/apt/lists/*
COPY init.sql /docker-entrypoint-initdb.d/01-init.sql
RUN chmod 0644 /docker-entrypoint-initdb.d/01-init.sql
