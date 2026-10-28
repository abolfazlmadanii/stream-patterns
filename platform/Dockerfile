FROM apache/flink:2.2.1-java17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && pip3 install --no-cache-dir apache-flink==2.2.1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER flink
