FROM phusion/baseimage:noble-1.0.2

ARG PHP_VERSION=8.3

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    install_clean \
        ca-certificates \
        software-properties-common && \
    add-apt-repository ppa:ondrej/php && \
    apt-get update && \
    install_clean \
        autoconf \
        build-essential \
        git \
        libssl-dev \
        pkg-config \
        zlib1g-dev \
        php${PHP_VERSION}-cli \
        php${PHP_VERSION}-dev \
        php${PHP_VERSION}-xml

RUN for VERSION in v1.7.0 v1.9.2 v2.6.0 v2.14.1; do \
        git clone --depth 1 --branch ${VERSION} https://github.com/confluentinc/librdkafka.git /tmp/librdkafka-${VERSION} && \
        cd /tmp/librdkafka-${VERSION} && \
        ./configure --prefix=/opt/librdkafka/${VERSION} && \
        make -j$(nproc) && \
        make install && \
        cd / && \
        rm -rf /tmp/librdkafka-${VERSION}; \
    done

COPY .docker/use-librdkafka /usr/local/bin/use-librdkafka
RUN chmod +x /usr/local/bin/use-librdkafka && \
    use-librdkafka v2.14.1

WORKDIR /src
