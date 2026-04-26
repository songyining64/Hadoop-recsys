FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
USER root
RUN sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list \
  && sed -i 's/security.debian.org/archive.debian.org/g' /etc/apt/sources.list \
  && sed -i '/stretch-updates/d' /etc/apt/sources.list \
  && apt-get -o Acquire::Check-Valid-Until=false update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3 \
  && rm -rf /var/lib/apt/lists/*
USER root
