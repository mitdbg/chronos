FROM e2bdev/base

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        nfs-common postgresql-client ripgrep && \
    rm -rf /var/lib/apt/lists/*
RUN mkdir -p /mnt/chronos
