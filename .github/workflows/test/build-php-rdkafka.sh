#!/bin/sh

set -e

echo "Building php-rdkafka with PHP version:"
php --version

cd php-rdkafka

CODE_VERSION="$(grep PHP_RDKAFKA_VERSION php_rdkafka.h|cut -d'"' -f2)"
PACKAGE_VERSION="$(grep -m 1 '<release>' package.xml|cut -d'>' -f2|cut -d'<' -f1)"

if ! [ "$CODE_VERSION" = "$PACKAGE_VERSION" ]; then
    printf "Version in php_rdkafka.h does not match version in package.xml: '%s' vs '%s'\n" "$CODE_VERSION" "$PACKAGE_VERSION" >&2
    exit 1
fi

php tools/update-package-contents.php

pecl package

if [ "$MEMORY_CHECK" = "1" ] || [ "$ARCH" = "X32" ]; then
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
