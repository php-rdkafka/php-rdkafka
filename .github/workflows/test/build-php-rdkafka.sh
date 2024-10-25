#!/bin/sh

set -e

echo "Building php-rdkafka with PHP version:"
php --version

cd php-rdkafka

PACKAGE_VERSION="$(grep -m 1 '<release>' package.xml|cut -d'>' -f2|cut -d'<' -f1)"

pecl package

if [ $MEMORY_CHECK -eq 1 ] || [ "$ARCH" = "X32" ]; then
    export CFLAGS="$CFLAGS -Wall -Werror -Wno-deprecated-declarations"
fi

if [ "$ARCH" = "X32" ]; then
    export CC="${CC:-gcc} -m32"
    export CFLAGS="$CFLAGS -m32"
    export CXXFLAGS="$CXXFLAGS -m32"
    export LDFLAGS="$LDFLAGS -m32"
fi

sudo -E pecl install "./rdkafka-$PACKAGE_VERSION.tgz"

echo "extension=rdkafka.so"|sudo tee /usr/local/etc/php/rdkafka.ini >/dev/null
