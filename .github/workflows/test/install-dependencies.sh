#!/bin/sh

set -ex

export DEBIAN_FRONTEND=noninteractive

DEPS="zlib1g-dev libssl-dev"

if [ "$ARCH" = "X32" ]; then
    sudo dpkg --add-architecture i386
    DEPS="$DEPS gcc-multilib g++-multilib zlib1g-dev:i386 libssl-dev:i386"
fi

if [ "$MEMORY_CHECK" = "1" ]; then
    DEPS="$DEPS valgrind"
fi

if [ -n "$DEPS" ]; then
    sudo apt-get update
    sudo apt-get -y install $DEPS
fi
