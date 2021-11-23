# ===================
# ===== Builder =====
# ===================

FROM harbor.vectbase.com/library/clickhouse-server-build:21.11.3 AS builder

# Reconstruct source tree inside docker
RUN mkdir /clickhouse-server
WORKDIR /clickhouse-server
RUN mv /contrib .
ADD . .

# Build clickhouse-server binary
RUN /build.sh

# ====================
# ==== Clickhouse ====
# ====================

FROM harbor.vectbase.com/library/ubuntu-for-clickhouse:20.04

MAINTAINER "Vectbase"

LABEL name="ClickHouse server" \
      maintainer="support@altinity.com" \
      version="21.11.3" \
      release="1" \
      summary="ClickHouse server" \
      description="ClickHouse server"

WORKDIR /

ARG repository="deb https://repo.clickhouse.com/deb/stable/ main/"
ARG version=21.11.3.*


# see https://github.com/moby/moby/issues/4032#issuecomment-192327844
ARG DEBIAN_FRONTEND=noninteractive

# user/group precreated explicitly with fixed uid/gid on purpose.
# It is especially important for rootless containers: in that case entrypoint
# can't do chown and owners of mounted volumes should be configured externally.
# We do that in advance at the begining of Dockerfile before any packages will be
# installed to prevent picking those uid / gid by some unrelated software.
# The same uid / gid (101) is used both for alpine and ubuntu.

RUN sed -i 's|http://archive|http://cn.archive|g' /etc/apt/sources.list && mkdir -p /tmp/clickhouse_binary

COPY --from=builder /clickhouse-server/build_docker/programs/clickhouse /tmp/clickhouse_binary/clickhouse

RUN groupadd -r clickhouse --gid=101 \ 
&& useradd -r -g clickhouse --uid=101 --home-dir=/var/lib/clickhouse --shell=/bin/bash clickhouse \
&& mkdir -p /etc/apt/sources.list.d \
&& echo $repository > /etc/apt/sources.list.d/clickhouse.list \
&& chmod +x /tmp/clickhouse_binary/clickhouse \
&& /tmp/clickhouse_binary/clickhouse install --user "clickhouse" --group "clickhouse" \
&& /tmp/clickhouse_binary/clickhouse local -q 'SELECT * FROM system.build_options' \
&& rm -rf \
   /var/lib/apt/lists/* \
   /var/cache/debconf \
   /tmp/* \
&& apt-get clean \
&& mkdir -p /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server \
&& chmod ugo+Xrw -R /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server

# we need to allow "others" access to clickhouse folder, because docker container
# can be started with arbitrary uid (openshift usecase)

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV TZ UTC

RUN mkdir /docker-entrypoint-initdb.d

COPY --from=builder /clickhouse-server/docker/server/docker_related_config.xml /etc/clickhouse-server/config.d/
COPY --from=builder /clickhouse-server/docker/server/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9000 8123 9009
VOLUME /var/lib/clickhouse

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml

ENTRYPOINT ["/entrypoint.sh"]
