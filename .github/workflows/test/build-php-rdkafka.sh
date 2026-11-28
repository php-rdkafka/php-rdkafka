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

pecl package

tar -tzf "rdkafka-$PACKAGE_VERSION.tgz"|sed -n "s@^rdkafka-$PACKAGE_VERSION/tests/@@p"|sort > package-test-files
find tests/ -type f|sed 's@^tests/@@'|sort > repository-test-files

if ! diff -u repository-test-files package-test-files; then
    echo "Some test files are missing from package.xml (see diff above)" >&2
    exit 1
fi

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
