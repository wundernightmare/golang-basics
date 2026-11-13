# PgBouncer for the libs/pgx integration tests (pgbouncer_test.go): the
# distribution's package on a plain Alpine, nothing else. The configuration is
# copied in by the test, one per container.
ARG DOCKER_HUB=docker.io
ARG ALPINE_VERSION=3.22
FROM ${DOCKER_HUB}/library/alpine:${ALPINE_VERSION}

# hadolint ignore=DL3018
RUN apk add --no-cache pgbouncer \
    && (id pgbouncer >/dev/null 2>&1 || adduser -D -H pgbouncer) \
    && mkdir -p /etc/pgbouncer /var/run/pgbouncer \
    && chown -R pgbouncer:pgbouncer /etc/pgbouncer /var/run/pgbouncer

USER pgbouncer
EXPOSE 6432
ENTRYPOINT ["pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
