# PgBouncer image is not present locally; built on top of the already-present alpine base.
ARG DOCKER_HUB=docker.io
FROM ${DOCKER_HUB}/library/alpine:3.22
RUN apk add --no-cache pgbouncer && adduser -D -H pgbouncer
USER pgbouncer
ENTRYPOINT ["pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
